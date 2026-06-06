from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
from contextlib import suppress
from datetime import date, datetime
from typing import Any

from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    ICS_EXTENSION,
    LOCAL_CALENDAR_PREFIX,
    LOCAL_CALENDAR_STORAGE_PATH,
    SERVICE_ADD_EVENT,
    SERVICE_DELETE_EVENT,
    SERVICE_IMPORT_EVENTS,
    SERVICE_LIST_EVENTS,
    SERVICE_UPDATE_EVENT,
)
from .helpers import (
    _coerce_all_day_end,
    _coerce_date,
    _coerce_dt,
    _coerce_local_floating_dt,
    _component_tzid,
    _dt_from_ical,
    _event_end_dt,
    _ical_property_value,
    _is_all_day_component,
    _iso_ical_value,
    _match_event,
    _uid_from_call_data,
)
from .service_schemas import (
    ADD_EVENT_SCHEMA,
    DELETE_EVENT_SCHEMA,
    IMPORT_EVENTS_SCHEMA,
    LIST_EVENTS_SCHEMA,
    UPDATE_EVENT_SCHEMA,
)

_LOGGER = logging.getLogger(__name__)


def _local_calendar_ics_path(hass: HomeAssistant, slug: str) -> str:
    return hass.config.path(
        LOCAL_CALENDAR_STORAGE_PATH,
        f"{LOCAL_CALENDAR_PREFIX}{slug}{ICS_EXTENSION}",
    )


async def _find_ics_path_for_calendar(hass: HomeAssistant, calendar_entity_id: str) -> str:
    """Resolve Local Calendar .ics path using the entity's storage_key."""
    ent_reg = er.async_get(hass)
    entity = ent_reg.async_get(calendar_entity_id)

    if not entity:
        raise ServiceValidationError(f"Entity not found in registry: {calendar_entity_id}")

    if entity.platform != "local_calendar":
        raise ServiceValidationError(f"{calendar_entity_id} is not a Local Calendar entity")

    if not entity.config_entry_id:
        raise ServiceValidationError(f"{calendar_entity_id} has no config_entry_id")

    cfg = hass.config_entries.async_get_entry(entity.config_entry_id)
    if not cfg or cfg.domain != "local_calendar":
        raise ServiceValidationError(
            f"{calendar_entity_id} is not backed by local_calendar config entry"
        )

    storage_key = cfg.data.get("storage_key")
    if not storage_key:
        raise ServiceValidationError(
            f"Local Calendar config entry has no storage_key: {cfg.entry_id}"
        )

    path = _local_calendar_ics_path(hass, str(storage_key))
    if not await hass.async_add_executor_job(os.path.exists, path):
        raise ServiceValidationError(f"Local Calendar .ics file not found: {path}")

    return path


def _load_icalendar(path: str):
    from icalendar import Calendar

    with open(path, "rb") as f:
        data = f.read()
    return Calendar.from_ical(data)


def _load_import_icalendar(raw_ics: str):
    from icalendar import Calendar

    ics_text = raw_ics.strip()
    if not ics_text:
        raise ServiceValidationError("ics is required")

    try:
        cal = Calendar.from_ical(ics_text)
    except Exception as err:
        raise ServiceValidationError("Invalid ICS content.") from err

    if getattr(cal, "name", None) != "VCALENDAR":
        raise ServiceValidationError("ICS content must contain a VCALENDAR.")

    imported_event_uids: set[str] = set()
    imported_events = []
    imported_timezones = []

    for component in cal.subcomponents:
        if component.name == "VTIMEZONE":
            tzid = str(component.get("TZID", "")).strip()
            if not tzid:
                raise ServiceValidationError("Imported VTIMEZONE is missing TZID.")
            imported_timezones.append(component)
            continue

        if component.name != "VEVENT":
            continue

        uid = str(component.get("UID", "")).strip()
        if not uid:
            raise ServiceValidationError("Each imported event must have a UID.")
        if uid in imported_event_uids:
            raise ServiceValidationError(f"Duplicate UID in imported ICS content: {uid}")

        dtstart_prop = component.get("DTSTART")
        if dtstart_prop is None:
            raise ServiceValidationError(f"Imported event {uid} is missing DTSTART.")

        start_dt = _dt_from_ical(dtstart_prop)
        if start_dt is None:
            raise ServiceValidationError(f"Imported event {uid} has an invalid DTSTART.")

        dtend_prop = component.get("DTEND")
        if dtend_prop is not None:
            start_raw = _ical_property_value(dtstart_prop)
            end_raw = _ical_property_value(dtend_prop)
            if isinstance(start_raw, datetime) != isinstance(end_raw, datetime):
                raise ServiceValidationError(
                    f"Imported event {uid} must use matching DTSTART/DTEND value types."
                )

        end_dt = _event_end_dt(component)
        if end_dt is not None and end_dt <= start_dt:
            raise ServiceValidationError(f"Imported event {uid} must end after it starts.")

        # All-day 00:00:00 logic: if DTSTART and DTEND are both at 00:00:00
        # and duration is 24h, treat as all-day
        end_dt_val = _dt_from_ical(dtend_prop) if dtend_prop is not None else None
        if isinstance(start_dt, datetime) and isinstance(end_dt_val, datetime):
            start_time = start_dt.time()
            end_time = end_dt_val.time()
            duration = end_dt_val - start_dt
            if (
                start_time == end_time == datetime.min.time()
                and duration.days == 1
                and duration.seconds == 0
            ):
                # Convert to all-day (date only) and set VALUE=DATE param
                from icalendar import vDate

                all_day_start = date(start_dt.year, start_dt.month, start_dt.day)
                all_day_end = date(end_dt_val.year, end_dt_val.month, end_dt_val.day)
                component["DTSTART"] = vDate(all_day_start)
                component["DTEND"] = vDate(all_day_end)
                component["DTSTART"].params["VALUE"] = "DATE"
                component["DTEND"].params["VALUE"] = "DATE"
        imported_event_uids.add(uid)
        imported_events.append(component)

    if not imported_events:
        raise ServiceValidationError("ICS content must contain at least one VEVENT.")

    return imported_timezones, imported_events, imported_event_uids


def _write_icalendar_atomic(path: str, cal) -> None:
    """Write ICS safely: backup, atomic replace, best-effort fsync."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{path}.bak_{ts}"
    with suppress(Exception):
        shutil.copy2(path, backup)

    directory = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(prefix="ics_", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            data = cal.to_ical()
            # Some parsers are happier with a trailing newline
            if data and not data.endswith(b"\n"):
                data += b"\n"
            f.write(data)
            try:
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                pass

        os.replace(tmp_path, path)

        # Best-effort flush of directory entry
        try:
            dir_fd = os.open(directory, os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass

    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def _get_mtime(path: str) -> float | None:
    try:
        return os.path.getmtime(path)
    except Exception:
        return None


async def _async_get_mtime(hass: HomeAssistant, path: str) -> float | None:
    return await hass.async_add_executor_job(_get_mtime, path)


def _ics_file_lock(hass: HomeAssistant, path: str) -> asyncio.Lock:
    data = hass.data.setdefault(DOMAIN, {})
    locks = data.setdefault("_ics_file_locks", {})
    return locks.setdefault(path, asyncio.Lock())


async def _wait_for_mtime_change(
    hass: HomeAssistant,
    path: str,
    before: float | None,
    timeout_s: float = 2.0,
) -> None:
    if before is None:
        return
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        now = await _async_get_mtime(hass, path)
        if now is None or now != before:
            return
        await asyncio.sleep(0.1)


async def _reload_local_calendar_entries(hass: HomeAssistant) -> None:
    """Reload Local Calendar config entries so it re-reads .ics files."""
    entries = hass.config_entries.async_entries("local_calendar")
    if not entries:
        _LOGGER.debug("ICS_CALENDAR_TOOLS: no local_calendar config entries found to reload")
        return

    _LOGGER.debug("ICS_CALENDAR_TOOLS: reloading %d local_calendar config entries", len(entries))
    for entry in entries:
        try:
            await hass.config_entries.async_reload(entry.entry_id)
        except Exception as e:
            _LOGGER.warning(
                "ICS_CALENDAR_TOOLS: failed to reload local_calendar entry %s: %s",
                entry.entry_id,
                e,
            )


async def _force_refresh_after_edit(
    hass: HomeAssistant, cal_ent: str, ics_path: str, before_mtime: float | None
) -> None:
    # Ensure filesystem mtime has updated so Local Calendar reload reads fresh content
    await _wait_for_mtime_change(hass, ics_path, before_mtime, timeout_s=2.0)

    # Reload Local Calendar entries (preferred; avoids relying on a user script)
    await _reload_local_calendar_entries(hass)

    # Nudge the specific calendar entity the UI is showing (best-effort)
    with suppress(Exception):
        await hass.services.async_call(
            "homeassistant",
            "update_entity",
            {"entity_id": cal_ent},
            blocking=True,
        )


def _register_services(hass: HomeAssistant) -> None:
    """Register services once per HA runtime."""
    data = hass.data.setdefault(DOMAIN, {})
    if data.get("_services_registered"):
        return
    data["_services_registered"] = True

    async def handle_add(call: ServiceCall) -> None:
        from icalendar import Event, vRecur

        cal_ent = call.data["calendar"]

        summary = (call.data.get("summary") or "").strip()
        if not summary:
            raise ServiceValidationError("summary is required")

        desc = call.data.get("description")
        loc = call.data.get("location")
        all_day = bool(call.data.get("all_day", False))
        start_val = call.data.get("start")
        end_val = call.data.get("end")
        rrule_raw = (call.data.get("rrule") or "").strip()

        path = await _find_ics_path_for_calendar(hass, cal_ent)

        async with _ics_file_lock(hass, path):
            before_mtime = await _async_get_mtime(hass, path)

            cal = await hass.async_add_executor_job(_load_icalendar, path)

            ev = Event()
            uid = (
                f"{dt_util.utcnow().strftime('%Y%m%dT%H%M%SZ')}-{os.urandom(4).hex()}@homeassistant"
            )
            ev.add("uid", uid)
            ev.add("summary", summary)

            if desc:
                ev.add("description", str(desc))
            if loc:
                ev.add("location", str(loc))

            if all_day:
                sdt = _coerce_date(start_val)
                edt = _coerce_all_day_end(end_val, sdt)
                ev.add("dtstart", sdt)
                ev.add("dtend", edt)
            else:
                sdt = _coerce_local_floating_dt(start_val)
                edt = _coerce_local_floating_dt(end_val)
                if edt <= sdt:
                    raise ServiceValidationError("end must be after start for non all-day events.")
                ev.add("dtstart", sdt)
                ev.add("dtend", edt)

            if rrule_raw:
                if rrule_raw.upper().startswith("RRULE:"):
                    rrule_raw = rrule_raw.split(":", 1)[1].strip()
                try:
                    ev.add("rrule", vRecur.from_ical(rrule_raw))
                except Exception as e:
                    raise ServiceValidationError(f"Invalid RRULE: {rrule_raw}") from e

            cal.add_component(ev)

            await hass.async_add_executor_job(_write_icalendar_atomic, path, cal)
            await _force_refresh_after_edit(hass, cal_ent, path, before_mtime)

    async def handle_delete(call: ServiceCall) -> None:
        cal_ent = call.data["calendar"]
        _LOGGER.debug("ICS_CALENDAR_TOOLS delete_event call data=%s", dict(call.data))

        uid = _uid_from_call_data(call.data)
        summary = call.data.get("summary")
        start_val = call.data.get("start")
        end_val = call.data.get("end")
        if uid is None and summary is None and start_val is None and end_val is None:
            raise ServiceValidationError(
                "Delete requires uid, or at least one fallback matcher: summary/start/end."
            )

        start = _coerce_dt(start_val) if start_val is not None else None
        end = _coerce_dt(end_val) if end_val is not None else None

        path = await _find_ics_path_for_calendar(hass, cal_ent)

        async with _ics_file_lock(hass, path):
            before_mtime = await _async_get_mtime(hass, path)

            cal = await hass.async_add_executor_job(_load_icalendar, path)

            removed = 0
            kept = []
            for comp in cal.subcomponents:
                if _match_event(comp, uid, summary, start, end):
                    removed += 1
                else:
                    kept.append(comp)

            if removed == 0:
                raise ServiceValidationError("No matching event found to delete.")

            if removed > 1 and not uid:
                raise ServiceValidationError(
                    "Multiple matches found; provide uid to delete precisely."
                )

            from icalendar import Calendar

            new_cal = Calendar()
            for k, v in cal.items():
                new_cal.add(k, v)
            for comp in kept:
                new_cal.add_component(comp)

            await hass.async_add_executor_job(_write_icalendar_atomic, path, new_cal)
            await _force_refresh_after_edit(hass, cal_ent, path, before_mtime)

    async def handle_update(call: ServiceCall) -> None:
        cal_ent = call.data["calendar"]
        _LOGGER.debug("ICS_CALENDAR_TOOLS update_event call data=%s", dict(call.data))

        uid = _uid_from_call_data(call.data)
        new_summary = call.data.get("summary")
        new_start_val = call.data.get("start")
        new_end_val = call.data.get("end")
        new_loc = call.data.get("location")
        new_desc = call.data.get("description")
        rrule_raw = (call.data.get("rrule") or "").strip()

        if not uid:
            raise ServiceValidationError("Update requires uid/id/event_id (a stable identifier).")
        if not any(
            key in call.data
            for key in ("summary", "start", "end", "location", "description", "rrule")
        ):
            raise ServiceValidationError("Update requires at least one field to change.")

        path = await _find_ics_path_for_calendar(hass, cal_ent)

        async with _ics_file_lock(hass, path):
            before_mtime = await _async_get_mtime(hass, path)

            cal = await hass.async_add_executor_job(_load_icalendar, path)

            updated = 0
            for comp in cal.subcomponents:
                if comp.name != "VEVENT":
                    continue
                ev_uid = str(comp.get("UID", "")).strip()
                if ev_uid != str(uid).strip():
                    continue

                all_day_event = _is_all_day_component(comp)

                if new_summary is not None:
                    comp["SUMMARY"] = new_summary
                if new_start_val is not None and comp.get("DTSTART") is not None:
                    comp["DTSTART"].dt = (
                        _coerce_date(new_start_val)
                        if all_day_event
                        else _coerce_local_floating_dt(new_start_val)
                    )
                if new_end_val is not None:
                    if all_day_event:
                        updated_start_raw = _ical_property_value(comp.get("DTSTART"))
                        if not isinstance(updated_start_raw, date) or isinstance(
                            updated_start_raw, datetime
                        ):
                            raise ServiceValidationError("All-day event DTSTART must be a date.")
                        new_end = _coerce_all_day_end(new_end_val, updated_start_raw)
                    else:
                        new_end = _coerce_local_floating_dt(new_end_val)
                    if comp.get("DTEND") is not None:
                        comp["DTEND"].dt = new_end
                if (
                    new_end_val is not None
                    and comp.get("DTEND") is None
                    and comp.get("DTSTART") is not None
                ):
                    try:
                        comp["DTEND"] = new_end
                        if comp.get("DURATION") is not None:
                            del comp["DURATION"]
                    except Exception:
                        pass
                if new_loc is not None:
                    comp["LOCATION"] = new_loc
                if new_desc is not None:
                    comp["DESCRIPTION"] = new_desc
                if rrule_raw:
                    from icalendar import vRecur

                    rr = rrule_raw
                    if rr.upper().startswith("RRULE:"):
                        rr = rr.split(":", 1)[1].strip()
                    try:
                        comp["RRULE"] = vRecur.from_ical(rr)
                    except Exception as e:
                        raise ServiceValidationError(f"Invalid RRULE: {rr}") from e
                elif "rrule" in call.data:
                    try:
                        if comp.get("RRULE") is not None:
                            del comp["RRULE"]
                    except Exception:
                        pass

                updated += 1

                updated_start = _dt_from_ical(comp.get("DTSTART"))
                updated_end = _event_end_dt(comp)
                if (
                    updated_start is not None
                    and updated_end is not None
                    and updated_end <= updated_start
                ):
                    raise ServiceValidationError("Updated event end must be after start.")

            if updated == 0:
                raise ServiceValidationError("No matching UID found to update.")

            await hass.async_add_executor_job(_write_icalendar_atomic, path, cal)
            await _force_refresh_after_edit(hass, cal_ent, path, before_mtime)

    async def handle_list(call: ServiceCall) -> dict[str, Any]:
        cal_ent = call.data["calendar"]

        start_val = call.data.get("start")
        end_val = call.data.get("end")
        limit: int = call.data.get("limit") or 0

        start_filter = _coerce_dt(start_val) if start_val is not None else None
        end_filter = _coerce_dt(end_val) if end_val is not None else None

        path = await _find_ics_path_for_calendar(hass, cal_ent)
        cal = await hass.async_add_executor_job(_load_icalendar, path)

        events: list[dict[str, Any]] = []
        for comp in cal.subcomponents:
            if comp.name != "VEVENT":
                continue

            start_dt = _dt_from_ical(comp.get("DTSTART"))
            end_dt = _event_end_dt(comp)

            if (
                start_filter
                and end_dt
                and dt_util.as_local(end_dt) < dt_util.as_local(start_filter)
            ):
                continue
            if (
                start_filter
                and not end_dt
                and start_dt
                and dt_util.as_local(start_dt) < dt_util.as_local(start_filter)
            ):
                continue
            if (
                end_filter
                and start_dt
                and dt_util.as_local(start_dt) > dt_util.as_local(end_filter)
            ):
                continue

            start_raw = getattr(comp.get("DTSTART"), "dt", None)
            item: dict[str, Any] = {
                "uid": str(comp.get("UID", "")).strip(),
                "summary": str(comp.get("SUMMARY", "")),
                "start": _iso_ical_value(comp.get("DTSTART")),
                "end": _iso_ical_value(comp.get("DTEND"))
                or (end_dt.isoformat() if end_dt else None),
                "all_day": isinstance(start_raw, date) and not isinstance(start_raw, datetime),
                "description": str(comp.get("DESCRIPTION", "")),
                "location": str(comp.get("LOCATION", "")),
                "rrule": str(comp.get("RRULE", "")),
            }
            events.append(item)

        events.sort(key=lambda ev: ev.get("start") or "")
        if limit > 0:
            events = events[:limit]

        return {"calendar": cal_ent, "count": len(events), "events": events}

    async def handle_import(call: ServiceCall) -> None:
        cal_ent = call.data["calendar"]
        clear_existing_events = bool(call.data.get("clear_existing_events", False))
        raw_ics = call.data["ics"]

        path = await _find_ics_path_for_calendar(hass, cal_ent)
        (
            imported_timezones,
            imported_events,
            imported_event_uids,
        ) = await hass.async_add_executor_job(_load_import_icalendar, raw_ics)

        async with _ics_file_lock(hass, path):
            before_mtime = await _async_get_mtime(hass, path)

            cal = await hass.async_add_executor_job(_load_icalendar, path)

            from icalendar import Calendar

            new_cal = Calendar()
            for key, value in cal.items():
                new_cal.add(key, value)

            existing_event_uids: set[str] = set()
            existing_timezones: set[str] = set()

            for component in cal.subcomponents:
                if component.name == "VEVENT":
                    existing_uid = str(component.get("UID", "")).strip()
                    if existing_uid:
                        existing_event_uids.add(existing_uid)
                    if clear_existing_events:
                        continue
                elif component.name == "VTIMEZONE":
                    tzid = _component_tzid(component)
                    if tzid:
                        existing_timezones.add(tzid)

                new_cal.add_component(component)

            duplicate_uids = (
                sorted(imported_event_uids & existing_event_uids)
                if not clear_existing_events
                else []
            )
            if duplicate_uids:
                raise ServiceValidationError(
                    "Imported ICS content contains UID values that already exist "
                    "in the selected calendar: "
                    + ", ".join(duplicate_uids[:5])
                    + ("..." if len(duplicate_uids) > 5 else "")
                )

            for component in imported_timezones:
                tzid = _component_tzid(component)
                if tzid and tzid in existing_timezones:
                    continue
                new_cal.add_component(component)
                if tzid:
                    existing_timezones.add(tzid)

            for component in imported_events:
                new_cal.add_component(component)

            await hass.async_add_executor_job(_write_icalendar_atomic, path, new_cal)
            await _force_refresh_after_edit(hass, cal_ent, path, before_mtime)

    hass.services.async_register(
        DOMAIN,
        SERVICE_LIST_EVENTS,
        handle_list,
        schema=LIST_EVENTS_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_ADD_EVENT,
        handle_add,
        schema=ADD_EVENT_SCHEMA,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_UPDATE_EVENT,
        handle_update,
        schema=UPDATE_EVENT_SCHEMA,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_DELETE_EVENT,
        handle_delete,
        schema=DELETE_EVENT_SCHEMA,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_IMPORT_EVENTS,
        handle_import,
        schema=IMPORT_EVENTS_SCHEMA,
    )


async def async_setup(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    # YAML configuration is not supported; services are registered from config entry setup.
    return True


async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    _LOGGER.debug("ICS_CALENDAR_TOOLS: setup entry %s", entry.entry_id)
    _register_services(hass)
    return True


async def async_unload_entry(hass: HomeAssistant, entry) -> bool:
    # Services remain registered for the runtime; nothing to unload.
    return True
