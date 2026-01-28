from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
from datetime import datetime, timedelta
from typing import Any, Mapping

from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


def _to_dt(value: str) -> datetime:
    dt = dt_util.parse_datetime(value)
    if dt is None:
        raise ValueError(f"Invalid datetime: {value}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
    return dt


def _find_ics_path_for_calendar(hass: HomeAssistant, calendar_entity_id: str) -> str:
    """Map calendar entity_id -> local_calendar .ics path (EXACT ONLY).

    Local Calendar stores files like:
      /config/.storage/local_calendar.<slug>.ics

    IMPORTANT:
    Do NOT fall back by friendly/original name. Calendar entities like
    'calendar.daisy_2' can share the same display name as a local calendar
    'calendar.daisy', and name-based fallback will write to the wrong file.
    """
    slug = calendar_entity_id.split(".", 1)[1]
    path = f"/config/.storage/local_calendar.{slug}.ics"
    if os.path.exists(path):
        return path
    raise FileNotFoundError(f"Could not find .ics file for {calendar_entity_id}. Tried {path}.")


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
        desc = call.data.get("description") or ""
        loc = call.data.get("location") or ""
        all_day = bool(call.data.get("all_day", False))
        start_raw = str(call.data.get("start") or "").strip()
        end_raw = str(call.data.get("end") or "").strip()
        rrule_raw = str(call.data.get("rrule") or "").strip()

        if not start_raw or not end_raw:
            raise ValueError("add_event requires start and end.")

        # If this is a Local Calendar entity, write directly to its .ics.
        try:
            path = _find_ics_path_for_calendar(hass, cal_ent)
        except FileNotFoundError:
            # Not a Local Calendar (.ics). Use HA calendar services for the selected entity.
            if all_day:
                await hass.services.async_call(
                    "calendar",
                    "create_event",
                    {
                        "entity_id": cal_ent,
                        "summary": summary,
                        "description": desc,
                        "location": loc,
                        "start_date": start_raw,
                        "end_date": end_raw,
                    },
                    blocking=True,
                )
            else:
                await hass.services.async_call(
                    "calendar",
                    "create_event",
                    {
                        "entity_id": cal_ent,
                        "summary": summary,
                        "description": desc,
                        "location": loc,
                        "start_date_time": start_raw,
                        "end_date_time": end_raw,
                    },
                    blocking=True,
                )

            # If no RRULE requested, we're done.
            if not rrule_raw:
                try:
                    await hass.services.async_call(
                        "homeassistant",
                        "update_entity",
                        {"entity_id": cal_ent},
                        blocking=False,
                    )
                except Exception:
                    pass
                return

            # RRULE requested: create base event, then poll for UID, then update with RRULE.
            # This relies on the provider exposing UID via calendar.get_events shortly after creation.
            def _best_uid(events: list[dict[str, Any]]) -> str | None:
                # Match by summary; prefer the closest start time if possible.
                want_start_dt = None
                try:
                    if all_day:
                        d = dt_util.parse_date(start_raw)
                        want_start_dt = dt_util.start_of_local_day(d) if d else None
                    else:
                        want_start_dt = _to_dt(start_raw.replace(" ", "T"))
                except Exception:
                    want_start_dt = None

                best_uid = None
                best_delta = None

                for e in events:
                    if (e.get("summary") or "") != summary:
                        continue
                    uid = e.get("uid") or e.get("UID") or e.get("id") or e.get("event_id")
                    if not uid:
                        continue

                    if want_start_dt is None:
                        return str(uid)

                    e_start = e.get("start")
                    e_start_dt = None
                    try:
                        if isinstance(e_start, str):
                            e_start_dt = dt_util.parse_datetime(e_start) or dt_util.parse_datetime(e_start.replace(" ", "T"))
                        elif isinstance(e_start, dict):
                            if "dateTime" in e_start:
                                e_start_dt = dt_util.parse_datetime(e_start["dateTime"])
                            elif "date" in e_start:
                                d = dt_util.parse_date(e_start["date"])
                                e_start_dt = dt_util.start_of_local_day(d) if d else None
                    except Exception:
                        e_start_dt = None

                    if e_start_dt is None:
                        best_uid = best_uid or str(uid)
                        continue

                    try:
                        delta = abs(dt_util.as_local(e_start_dt) - dt_util.as_local(want_start_dt))
                    except Exception:
                        best_uid = best_uid or str(uid)
                        continue

                    if best_delta is None or delta < best_delta:
                        best_delta = delta
                        best_uid = str(uid)

                return best_uid

            # Search window with slack
            try:
                if all_day:
                    s_date = dt_util.parse_date(start_raw) or _to_dt(start_raw).date()
                    e_date = dt_util.parse_date(end_raw) or _to_dt(end_raw).date()
                    s_dt = dt_util.start_of_local_day(s_date) - timedelta(minutes=2)
                    e_dt = dt_util.start_of_local_day(e_date) + timedelta(days=1, minutes=2)
                else:
                    s_dt = _to_dt(start_raw.replace(" ", "T")) - timedelta(minutes=2)
                    e_dt = _to_dt(end_raw.replace(" ", "T")) + timedelta(minutes=2)
            except Exception:
                s_dt = dt_util.utcnow() - timedelta(days=1)
                e_dt = dt_util.utcnow() + timedelta(days=1)

            uid = None
            for _ in range(10):  # ~5 seconds total
                resp = await hass.services.async_call(
                    "calendar",
                    "get_events",
                    {
                        "entity_id": cal_ent,
                        "start_date_time": dt_util.as_local(s_dt).strftime("%Y-%m-%d %H:%M:%S"),
                        "end_date_time": dt_util.as_local(e_dt).strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    blocking=True,
                    return_response=True,
                )
                events = []
                if isinstance(resp, dict):
                    events = resp.get("events") or []
                uid = _best_uid(events) if isinstance(events, list) else None
                if uid:
                    break
                await asyncio.sleep(0.5)

            if not uid:
                raise ValueError(
                    "Created the base event, but could not retrieve its UID to apply RRULE. "
                    "This calendar provider may not expose UID via calendar.get_events."
                )

            rrule_clean = rrule_raw.strip()
            if rrule_clean.upper().startswith("RRULE:"):
                rrule_clean = rrule_clean.split(":", 1)[1].strip()

            await hass.services.async_call(
                "calendar",
                "update_event",
                {"entity_id": cal_ent, "uid": uid, "rrule": rrule_clean},
                blocking=True,
            )

            try:
                await hass.services.async_call(
                    "homeassistant",
                    "update_entity",
                    {"entity_id": cal_ent},
                    blocking=False,
                )
            except Exception:
                pass
            return

        # Local Calendar (.ics) path
        before_mtime = None
        try:
            before_mtime = os.path.getmtime(path)
        except Exception:
            pass

        cal = await hass.async_add_executor_job(_load_icalendar, path)

        from icalendar import Event

        ev = Event()
        # If we have an RRULE, we want a stable UID so delete/update work across reloads.
        uid = call.data.get("uid") or call.data.get("UID")
        if not uid:
            uid = f"{dt_util.utcnow().strftime('%Y%m%dT%H%M%SZ')}-{os.urandom(4).hex()}@ics_calendar_tools"
        ev.add("UID", str(uid))

        ev.add("SUMMARY", summary)
        if desc:
            ev.add("DESCRIPTION", desc)
        if loc:
            ev.add("LOCATION", loc)

        if all_day:
            # For all-day events, DTSTART/DTEND are dates (DTEND is exclusive in RFC5545,
            # but Local Calendar generally tolerates same-day DTEND; we keep caller intent).
            s_date = dt_util.parse_date(start_raw)
            e_date = dt_util.parse_date(end_raw)
            if s_date is None or e_date is None:
                raise ValueError("For all_day=true, start/end must be YYYY-MM-DD.")
            ev.add("DTSTART", s_date)
            ev.add("DTEND", e_date)
        else:
            s_dt = _to_dt(start_raw.replace(" ", "T"))
            e_dt = _to_dt(end_raw.replace(" ", "T"))
            ev.add("DTSTART", s_dt)
            ev.add("DTEND", e_dt)

        if rrule_raw:
            rrule_clean = rrule_raw.strip()
            if rrule_clean.upper().startswith("RRULE:"):
                rrule_clean = rrule_clean.split(":", 1)[1].strip()
            # icalendar accepts dict or string; string is fine as 'FREQ=...;...'
            ev["RRULE"] = rrule_clean

        cal.add_component(ev)

        await hass.async_add_executor_job(_write_icalendar_atomic, path, cal)
        await _force_refresh_after_edit(hass, cal_ent, path, before_mtime)

    async def handle_delete(call: ServiceCall) -> None:
        cal_ent = call.data["calendar"]
        _LOGGER.debug("ICS_CALENDAR_TOOLS delete_event call data=%s", dict(call.data))

        uid = _uid_from_call_data(call.data)
        summary = call.data.get("summary")
        start_s = call.data.get("start")
        end_s = call.data.get("end")

        start = _to_dt(start_s) if start_s else None
        end = _to_dt(end_s) if end_s else None

        try:
            path = _find_ics_path_for_calendar(hass, cal_ent)
        except FileNotFoundError:
            # Non-local calendar: we require a UID to delete precisely.
            if not uid:
                raise ValueError("delete_event on non-local calendars requires uid.")
            await hass.services.async_call(
                "calendar",
                "delete_event",
                {"entity_id": cal_ent, "uid": str(uid).strip()},
                blocking=True,
            )
            try:
                await hass.services.async_call(
                    "homeassistant",
                    "update_entity",
                    {"entity_id": cal_ent},
                    blocking=False,
                )
            except Exception:
                pass
            return

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
        new_rrule = str(call.data.get("rrule") or "").strip()

        new_start = _to_dt(new_start_s) if new_start_s else None
        new_end = _to_dt(new_end_s) if new_end_s else None

        if not uid:
            raise ValueError("Update requires uid/id/event_id (a stable identifier).")

        try:
            path = _find_ics_path_for_calendar(hass, cal_ent)
        except FileNotFoundError:
            # Non-local calendar: delegate to HA calendar.update_event
            payload: dict[str, Any] = {"entity_id": cal_ent, "uid": str(uid).strip()}
            if new_summary is not None:
                payload["summary"] = new_summary
            if new_desc is not None:
                payload["description"] = new_desc
            if new_loc is not None:
                payload["location"] = new_loc
            if new_start_s is not None and new_end_s is not None:
                # We don't know if it's all-day; pass both fields if caller provided.
                # Most providers accept start/end or start_date_time/end_date_time.
                payload["start"] = new_start_s
                payload["end"] = new_end_s
            if new_rrule:
                if new_rrule.upper().startswith("RRULE:"):
                    new_rrule = new_rrule.split(":", 1)[1].strip()
                payload["rrule"] = new_rrule
            await hass.services.async_call("calendar", "update_event", payload, blocking=True)
            try:
                await hass.services.async_call("homeassistant", "update_entity", {"entity_id": cal_ent}, blocking=False)
            except Exception:
                pass
            return

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
            if new_rrule:
                if new_rrule.upper().startswith("RRULE:"):
                    new_rrule = new_rrule.split(":", 1)[1].strip()
                comp["RRULE"] = new_rrule

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
