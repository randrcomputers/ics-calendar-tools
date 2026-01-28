from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Mapping

from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


def _to_date(value: str) -> date:
    d = dt_util.parse_date(value)
    if d is None:
        dt = dt_util.parse_datetime(value)
        if dt is not None:
            d = dt.date()
    if d is None:
        raise ValueError(f"Invalid date: {value}")
    return d


def _to_dt(value: str) -> datetime:
    dt = dt_util.parse_datetime(value)
    if dt is None:
        raise ValueError(f"Invalid datetime: {value}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
    return dt


def _find_ics_path_for_calendar(hass: HomeAssistant, calendar_entity_id: str) -> str:
    """Map calendar entity_id -> local_calendar .ics path (exact match only).

    Local Calendar stores files like:
      /config/.storage/local_calendar.<slug>.ics

    IMPORTANT:
      - We intentionally do NOT use any name-based fallback. That fallback can
        accidentally map a non-local calendar entity (e.g., Google) to a local
        .ics file with the same display name.
    """
    slug = calendar_entity_id.split(".", 1)[1]
    path = f"/config/.storage/local_calendar.{slug}.ics"
    if os.path.exists(path):
        return path

    raise FileNotFoundError(
        f"Could not find Local Calendar .ics for {calendar_entity_id}. "
        f"Tried: {path}. (No name-based fallback is used.)"
    )

def _local_ics_path_or_none(calendar_entity_id: str) -> str | None:
    """Return Local Calendar .ics path if it exists for this entity_id."""
    try:
        slug = calendar_entity_id.split(".", 1)[1]
    except Exception:
        return None
    path = f"/config/.storage/local_calendar.{slug}.ics"
    return path if os.path.exists(path) else None



def _load_icalendar(path: str):
    from icalendar import Calendar

    with open(path, "rb") as f:
        data = f.read()
    return Calendar.from_ical(data)


def _write_icalendar_atomic(path: str, cal) -> None:
    """Write ICS safely: backup, atomic replace, best-effort fsync."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{path}.bak_{ts}"
    try:
        shutil.copy2(path, backup)
    except Exception:
        # If file didn't exist yet, ignore
        pass

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


def _normalize_summary(s: str | None) -> str | None:
    if s is None:
        return None
    s2 = " ".join(str(s).strip().split())
    return s2.lower()


def _uid_from_call_data(data: Mapping[str, Any]) -> str | None:
    """Accept UID from several common keys used by cards/integrations."""
    for k in (
        "uid",
        "UID",
        "id",
        "event_id",
        "eventId",
        "event_uid",
        "eventUid",
        "ical_uid",
        "icalUid",
    ):
        v = data.get(k)
        if v:
            return str(v).strip()
    return None


def _dt_from_ical(value) -> datetime | None:
    """Convert DTSTART/DTEND's .dt (date or datetime) to datetime (local)."""
    if value is None:
        return None
    v = getattr(value, "dt", value)
    if isinstance(v, datetime):
        return dt_util.as_local(v)
    # date -> treat as local start of day
    try:
        return dt_util.as_local(dt_util.start_of_local_day(v))
    except Exception:
        return None


def _event_end_dt(component) -> datetime | None:
    """Return event end datetime, handling DTEND or DURATION."""
    ev_end = component.get("DTEND")
    if ev_end:
        return _dt_from_ical(ev_end)
    dur = component.get("DURATION")
    if dur:
        try:
            start = _dt_from_ical(component.get("DTSTART"))
            if start is None:
                return None
            # icalendar stores DURATION as datetime.timedelta typically
            if isinstance(dur.dt, timedelta):
                return start + dur.dt
        except Exception:
            return None
    return None


def _match_event(component, uid: str | None, summary: str | None, start: datetime | None, end: datetime | None) -> bool:
    if component.name != "VEVENT":
        return False

    # UID match (preferred)
    if uid:
        ev_uid = str(component.get("UID", "")).strip()

        # Most of the time UID is exact match.
        if ev_uid == uid:
            return True

        # Some UIs pass composite IDs; be tolerant only when it looks safe.
        if len(uid) >= 12 and (uid in ev_uid or ev_uid in uid):
            return True

        return False

    # Fallback matching by summary/start/end (best-effort)
    if summary is not None:
        if _normalize_summary(str(component.get("SUMMARY", ""))) != _normalize_summary(summary):
            return False

    if start is not None:
        ev_start_dt = _dt_from_ical(component.get("DTSTART"))
        if ev_start_dt is None:
            return False
        if abs(dt_util.as_local(ev_start_dt) - dt_util.as_local(start)) > timedelta(minutes=1):
            return False

    if end is not None:
        ev_end_dt = _event_end_dt(component)
        if ev_end_dt is None:
            return False
        if abs(dt_util.as_local(ev_end_dt) - dt_util.as_local(end)) > timedelta(minutes=1):
            return False

    return True


def _all_local_calendar_ics_paths() -> list[str]:
    base = "/config/.storage"
    out: list[str] = []
    try:
        for name in os.listdir(base):
            if name.startswith("local_calendar.") and name.endswith(".ics"):
                out.append(os.path.join(base, name))
    except Exception:
        pass
    return out


def _ics_paths_containing_uid(uid: str) -> list[str]:
    """Fast-ish scan to find which local_calendar.*.ics contains a UID."""
    uid_s = str(uid).strip()
    needle1 = f"UID:{uid_s}".encode("utf-8")
    needle2 = b"UID;"
    uid_b = uid_s.encode("utf-8")

    matches: list[str] = []
    for path in _all_local_calendar_ics_paths():
        try:
            with open(path, "rb") as f:
                data = f.read()
            if needle1 in data or (needle2 in data and uid_b in data):
                matches.append(path)
        except Exception:
            continue
    return matches


async def _wait_for_mtime_change(path: str, before: float | None, timeout_s: float = 2.0) -> None:
    if before is None:
        return
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        try:
            now = os.path.getmtime(path)
            if now != before:
                return
        except Exception:
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
            _LOGGER.warning("ICS_CALENDAR_TOOLS: failed to reload local_calendar entry %s: %s", entry.entry_id, e)


async def _force_refresh_after_edit(hass: HomeAssistant, cal_ent: str, ics_path: str, before_mtime: float | None) -> None:
    # Ensure filesystem mtime has updated so Local Calendar reload reads fresh content
    await _wait_for_mtime_change(ics_path, before_mtime, timeout_s=2.0)

    # Reload Local Calendar entries (preferred; avoids relying on a user script)
    await _reload_local_calendar_entries(hass)

    # Nudge the specific calendar entity the UI is showing (best-effort)
    try:
        await hass.services.async_call(
            "homeassistant",
            "update_entity",
            {"entity_id": cal_ent},
            blocking=True,
        )
    except Exception:
        pass


def _register_services(hass: HomeAssistant) -> None:
    """Register services once per HA runtime."""
    data = hass.data.setdefault(DOMAIN, {})
    if data.get("_services_registered"):
        return
    data["_services_registered"] = True

    async def handle_add(call: ServiceCall) -> None:
        cal_ent = call.data["calendar"]
        _LOGGER.debug("ICS_CALENDAR_TOOLS add_event call data=%s", dict(call.data))

        summary = call.data.get("summary") or ""
        description = call.data.get("description")
        location = call.data.get("location")
        all_day = bool(call.data.get("all_day", False))
        start_s = call.data.get("start")
        end_s = call.data.get("end")
        rrule_s = call.data.get("rrule")

        if not start_s or not end_s:
            raise ValueError("add_event requires start and end.")

        ics_path = _local_ics_path_or_none(cal_ent)

        # --- Local Calendar (.ics) path exists -> write directly ---
        if ics_path:
            before_mtime = None
            try:
                before_mtime = os.path.getmtime(ics_path)
            except Exception:
                pass

            cal = await hass.async_add_executor_job(_load_icalendar, ics_path)

            ev = Event()
            ev.add("uid", call.data.get("uid") or str(uuid.uuid4()))
            ev.add("dtstamp", datetime.utcnow())

            ev.add("summary", summary)
            if description is not None:
                ev.add("description", description)
            if location is not None:
                ev.add("location", location)

            if all_day:
                ev.add("dtstart", _to_date(start_s))
                ev.add("dtend", _to_date(end_s))
            else:
                ev.add("dtstart", _to_dt(start_s))
                ev.add("dtend", _to_dt(end_s))

            if rrule_s is not None and str(rrule_s).strip():
                from icalendar import vRecur
                ev.add("rrule", vRecur.from_ical(str(rrule_s).strip()))

            cal.add_component(ev)
            await hass.async_add_executor_job(_write_icalendar_atomic, ics_path, cal)
            await _force_refresh_after_edit(hass, cal_ent, ics_path, before_mtime)
            return

        # --- Not a Local Calendar (.ics) -> use HA Calendar services ---
        # Create a single event first (calendar.create_event does not accept RRULE today).
        create_data: dict[str, Any] = {
            "entity_id": cal_ent,
            "summary": summary,
        }
        if description is not None:
            create_data["description"] = description
        if location is not None:
            create_data["location"] = location

        if all_day:
            create_data["start_date"] = _to_date(start_s).isoformat()
            create_data["end_date"] = _to_date(end_s).isoformat()
        else:
            create_data["start_date_time"] = _to_dt(start_s).strftime("%Y-%m-%d %H:%M:%S")
            create_data["end_date_time"] = _to_dt(end_s).strftime("%Y-%m-%d %H:%M:%S")

        await hass.services.async_call("calendar", "create_event", create_data, blocking=True)

        # If no RRULE requested, we are done.
        if not (rrule_s is not None and str(rrule_s).strip()):
            try:
                await hass.services.async_call(
                    "homeassistant",
                    "update_entity",
                    {"entity_id": cal_ent},
                    blocking=True,
                )
            except Exception:
                pass
            return

        # Best-effort: fetch the newly created event UID, then update with RRULE.
        uid = None
        try:
            # Query a narrow window around the new event.
            if all_day:
                win_start = _to_dt(f"{_to_date(start_s).isoformat()}T00:00:00")
                win_end = _to_dt(f"{_to_date(end_s).isoformat()}T23:59:59")
            else:
                win_start = _to_dt(start_s) - timedelta(minutes=2)
                win_end = _to_dt(end_s) + timedelta(minutes=2)

            resp = await hass.services.async_call(
                "calendar",
                "get_events",
                {
                    "entity_id": cal_ent,
                    "start_date_time": win_start.isoformat(),
                    "end_date_time": win_end.isoformat(),
                },
                blocking=True,
                return_response=True,
            )

            events = None
            if isinstance(resp, dict):
                if "events" in resp:
                    events = resp.get("events")
                elif cal_ent in resp and isinstance(resp.get(cal_ent), dict):
                    events = resp.get(cal_ent, {}).get("events")
            if events is None:
                _LOGGER.debug("ICS_CALENDAR_TOOLS add_event get_events response had no 'events': %s", resp)

            def _norm(s: Any) -> str:
                return (str(s).strip() if s is not None else "")

            def _same_start(ev: dict) -> bool:
                # HA event start can be "date" or "dateTime" depending on all_day
                st = ev.get("start") or {}
                if all_day:
                    return _norm(st.get("date")) == _to_date(start_s).isoformat()
                return _norm(st.get("dateTime"))[:19] == _to_dt(start_s).isoformat()[:19]

            if isinstance(events, list):
                for ev in events:
                    if not isinstance(ev, dict):
                        continue
                    if _norm(ev.get("summary")) != _norm(summary):
                        continue
                    if not _same_start(ev):
                        continue
                    uid = ev.get("uid") or ev.get("id")
                    if uid:
                        break
        except TypeError:
            _LOGGER.warning("ICS_CALENDAR_TOOLS add_event: return_response not supported on this HA version; cannot set RRULE on non-local calendars.")
        except Exception as e:
            _LOGGER.warning("ICS_CALENDAR_TOOLS add_event: failed to locate newly created event UID for RRULE update: %s", e)

        if not uid:
            raise ValueError(
                "Created the base event, but could not retrieve its UID to apply RRULE. "
                "This HA version/integration may not expose UID via calendar.get_events."
            )

        # Apply RRULE using calendar.update_event (works for providers that support recurrence, e.g. Google).
        await hass.services.async_call(
            "calendar",
            "update_event",
            {
                "entity_id": cal_ent,
                "uid": uid,
                "rrule": str(rrule_s).strip(),
            },
            blocking=True,
        )

        try:
            await hass.services.async_call(
                "homeassistant",
                "update_entity",
                {"entity_id": cal_ent},
                blocking=True,
            )
        except Exception:
            pass


    async def handle_delete(call: ServiceCall) -> None:
        cal_ent = call.data["calendar"]
        _LOGGER.debug("ICS_CALENDAR_TOOLS delete_event call data=%s", dict(call.data))

        uid = _uid_from_call_data(call.data)
        summary = call.data.get("summary")
        start_s = call.data.get("start")
        end_s = call.data.get("end")

        start = _to_dt(start_s) if start_s else None
        end = _to_dt(end_s) if end_s else None

        path = _find_ics_path_for_calendar(hass, cal_ent)
        before_mtime = None
        try:
            before_mtime = os.path.getmtime(path)
        except Exception:
            pass

        cal = await hass.async_add_executor_job(_load_icalendar, path)

        removed = 0
        kept = []
        for comp in cal.subcomponents:
            if _match_event(comp, uid, summary, start, end):
                removed += 1
            else:
                kept.append(comp)

        if removed == 0:
            if uid:
                paths = _ics_paths_containing_uid(str(uid).strip())
                if not paths:
                    raise ValueError("No matching event found to delete (UID not found in any local calendar).")

                deleted_any = False
                for p in paths:
                    cal2 = await hass.async_add_executor_job(_load_icalendar, p)

                    removed2 = 0
                    kept2 = []
                    for comp2 in cal2.subcomponents:
                        if _match_event(comp2, str(uid).strip(), None, None, None):
                            removed2 += 1
                        else:
                            kept2.append(comp2)

                    if removed2:
                        from icalendar import Calendar

                        new_cal2 = Calendar()
                        for k, v in cal2.items():
                            new_cal2.add(k, v)
                        for comp2 in kept2:
                            new_cal2.add_component(comp2)

                        await hass.async_add_executor_job(_write_icalendar_atomic, p, new_cal2)
                        deleted_any = True

                if not deleted_any:
                    raise ValueError("No matching event found to delete (UID search hit files but VEVENT not removed).")

                await _force_refresh_after_edit(hass, cal_ent, path, before_mtime)
                return

            raise ValueError("No matching event found to delete.")

        if removed > 1 and not uid:
            raise ValueError("Multiple matches found; provide uid to delete precisely.")

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
        new_start_s = call.data.get("start")
        new_end_s = call.data.get("end")
        new_loc = call.data.get("location")
        new_desc = call.data.get("description")

        new_start = _to_dt(new_start_s) if new_start_s else None
        new_end = _to_dt(new_end_s) if new_end_s else None

        if not uid:
            raise ValueError("Update requires uid/id/event_id (a stable identifier).")

        path = _find_ics_path_for_calendar(hass, cal_ent)
        before_mtime = None
        try:
            before_mtime = os.path.getmtime(path)
        except Exception:
            pass

        cal = await hass.async_add_executor_job(_load_icalendar, path)

        updated = 0
        for comp in cal.subcomponents:
            if comp.name != "VEVENT":
                continue
            ev_uid = str(comp.get("UID", "")).strip()
            if ev_uid != str(uid).strip():
                continue

            if new_summary is not None:
                comp["SUMMARY"] = new_summary
            if new_start is not None and comp.get("DTSTART") is not None:
                comp["DTSTART"].dt = new_start
            if new_end is not None and comp.get("DTEND") is not None:
                comp["DTEND"].dt = new_end
            # If event uses DURATION and caller gives end, convert to DTEND
            if new_end is not None and comp.get("DTEND") is None and comp.get("DTSTART") is not None:
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

            new_rrule = call.data.get("rrule")
            if new_rrule is not None:
                # Set or clear RRULE (RFC5545). Empty string clears the rule.
                if str(new_rrule).strip() == "":
                    if comp.get("RRULE") is not None:
                        del comp["RRULE"]
                else:
                    from icalendar import vRecur
                    comp["RRULE"] = vRecur.from_ical(str(new_rrule).strip())

            updated += 1

        if updated == 0:
            raise ValueError("No matching UID found to update.")

        await hass.async_add_executor_job(_write_icalendar_atomic, path, cal)
        await _force_refresh_after_edit(hass, cal_ent, path, before_mtime)

    hass.services.async_register(DOMAIN, "add_event", handle_add)
    hass.services.async_register(DOMAIN, "delete_event", handle_delete)
    hass.services.async_register(DOMAIN, "update_event", handle_update)


async def async_setup(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    # No YAML required. Services will be registered when the config entry is created.
    return True


async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    _LOGGER.debug("ICS_CALENDAR_TOOLS: setup entry %s", entry.entry_id)
    _register_services(hass)
    return True


async def async_unload_entry(hass: HomeAssistant, entry) -> bool:
    # Services remain registered for the runtime; nothing to unload.
    return True
