from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
from datetime import datetime
from typing import Any

from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

DOMAIN = "ics_calendar_tools"
_LOGGER = logging.getLogger(__name__)

# MUST match your script entity exactly (from your screenshot):
#   script.reload_local_calendars
RELOAD_SCRIPT_ENTITY_ID = "script.reload_local_calendars"


def _to_dt(value: str) -> datetime:
    dt = dt_util.parse_datetime(value)
    if dt is None:
        raise ValueError(f"Invalid datetime: {value}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
    return dt


def _find_ics_path_for_calendar(hass: HomeAssistant, calendar_entity_id: str) -> str:
    slug = calendar_entity_id.split(".", 1)[1]
    path = f"/config/.storage/local_calendar.{slug}.ics"
    if os.path.exists(path):
        return path

    ent_reg = er.async_get(hass)
    entry = ent_reg.async_get(calendar_entity_id)
    if entry and entry.original_name:
        guess = entry.original_name.strip().lower().replace(" ", "_")
        path2 = f"/config/.storage/local_calendar.{guess}.ics"
        if os.path.exists(path2):
            return path2

    raise FileNotFoundError(
        f"Could not find .ics file for {calendar_entity_id}. "
        f"Tried {path} (and a name-based fallback)."
    )


def _load_icalendar(path: str):
    from icalendar import Calendar

    with open(path, "rb") as f:
        data = f.read()
    return Calendar.from_ical(data)


def _write_icalendar_atomic(path: str, cal) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"{path}.bak_{ts}"
    shutil.copy2(path, backup)

    directory = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(prefix="ics_", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            data = cal.to_ical()
            if isinstance(data, str):
                data = data.encode('utf-8')
            if not data.endswith(b"
"):
                data += b"
"
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


def _match_event(component, uid: str | None, summary: str | None, start: datetime | None, end: datetime | None) -> bool:
    if component.name != "VEVENT":
        return False

    if uid:
        return str(component.get("UID", "")) == str(uid)

    if summary:
        if str(component.get("SUMMARY", "")) != summary:
            return False

    if start:
        ev_start = component.get("DTSTART")
        if not ev_start:
            return False
        ev_start_dt = ev_start.dt
        if not isinstance(ev_start_dt, datetime):
            ev_start_dt = dt_util.as_local(dt_util.start_of_local_day(ev_start_dt))
        if dt_util.as_local(ev_start_dt) != dt_util.as_local(start):
            return False

    if end:
        ev_end = component.get("DTEND")
        if not ev_end:
            return False
        ev_end_dt = ev_end.dt
        if not isinstance(ev_end_dt, datetime):
            ev_end_dt = dt_util.as_local(dt_util.start_of_local_day(ev_end_dt))
        if dt_util.as_local(ev_end_dt) != dt_util.as_local(end):
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
    """Reload all Local Calendar config entries so changes in .ics files are re-parsed."""
    entries = hass.config_entries.async_entries("local_calendar")
    if not entries:
        _LOGGER.warning("ICS_CALENDAR_TOOLS: no local_calendar config entries found to reload")
        return

    _LOGGER.warning("ICS_CALENDAR_TOOLS: reloading %d local_calendar config entr%s",
                    len(entries), "y" if len(entries) == 1 else "ies")
    # Reload sequentially to avoid storms / races on slower systems
    for entry in entries:
        try:
            await hass.config_entries.async_reload(entry.entry_id)
        except Exception as e:
            _LOGGER.error("ICS_CALENDAR_TOOLS: failed to reload local_calendar entry %s: %s", entry.entry_id, e)



async def _force_refresh_after_edit(hass: HomeAssistant, cal_ent: str, ics_path: str, before_mtime: float | None) -> None:
    # Ensure filesystem mtime has updated so Local Calendar reload reads fresh content
    await _wait_for_mtime_change(ics_path, before_mtime, timeout_s=2.0)

    # Reload Local Calendar integration entries (more reliable than update_entity/script hacks)
    try:
        await _reload_local_calendar_entries(hass)
    except Exception as e:
        _LOGGER.error("ICS_CALENDAR_TOOLS: local_calendar reload failed: %s", e)

    # Small pause to let entities settle before the UI fetches events again
    await asyncio.sleep(0.3)

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

    # Nudge the specific calendar entity the UI is showing
    try:
        await hass.services.async_call(
            "homeassistant",
            "update_entity",
            {"entity_id": cal_ent},
            blocking=True,
        )
    except Exception:
        pass


async def async_setup(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    # This log line is your “am I editing the right file?” proof
    _LOGGER.warning("ICS_CALENDAR_TOOLS LOADED from /config/custom_components/ics_calendar_tools/__init__.py")

    async def handle_delete(call: ServiceCall) -> None:
        cal_ent = call.data["calendar"]
        uid = call.data.get("uid")
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

                await _force_refresh_after_edit(hass, cal_ent, path, None)
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
        await _force_refresh_after_edit(hass, cal_ent, path, None)

    async def handle_update(call: ServiceCall) -> None:
        cal_ent = call.data["calendar"]
        uid = call.data.get("uid")

        new_summary = call.data.get("summary")
        new_start_s = call.data.get("start")
        new_end_s = call.data.get("end")
        new_loc = call.data.get("location")
        new_desc = call.data.get("description")

        new_start = _to_dt(new_start_s) if new_start_s else None
        new_end = _to_dt(new_end_s) if new_end_s else None

        if not uid:
            raise ValueError("Update requires uid (reliable) in this first version.")

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
            if str(comp.get("UID", "")) != str(uid):
                continue

            if new_summary is not None:
                comp["SUMMARY"] = new_summary
            if new_start is not None and comp.get("DTSTART") is not None:
                comp["DTSTART"].dt = new_start
            if new_end is not None and comp.get("DTEND") is not None:
                comp["DTEND"].dt = new_end
            if new_loc is not None:
                comp["LOCATION"] = new_loc
            if new_desc is not None:
                comp["DESCRIPTION"] = new_desc

            updated += 1

        if updated == 0:
            raise ValueError("No matching UID found to update.")

        await hass.async_add_executor_job(_write_icalendar_atomic, path, cal)
        await _force_refresh_after_edit(hass, cal_ent, path, None)

    hass.services.async_register(DOMAIN, "delete_event", handle_delete)
    hass.services.async_register(DOMAIN, "update_event", handle_update)
    return True
