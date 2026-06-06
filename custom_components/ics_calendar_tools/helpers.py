from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta
from typing import Any

import voluptuous as vol
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import config_validation as cv
from homeassistant.util import dt as dt_util

from .const import UID_DATA_KEYS


def _non_empty_string(value: Any) -> str:
    """Validate string input and reject whitespace-only values."""
    text = cv.string(value).strip()
    if not text:
        raise vol.Invalid("Value cannot be empty.")
    return text


def _to_dt(value: str) -> datetime:
    dt = dt_util.parse_datetime(value)
    if dt is None:
        parsed_date = dt_util.parse_date(value)
        if parsed_date is not None:
            dt = dt_util.start_of_local_day(parsed_date)
        else:
            raise ServiceValidationError(f"Invalid datetime: {value}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
    return dt


def _coerce_dt(value: Any) -> datetime:
    """Accept datetime object or string and return an aware local datetime."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
        return dt_util.as_local(value)
    if isinstance(value, date):
        return dt_util.start_of_local_day(value)
    return _to_dt(str(value).strip().replace(" ", "T"))


def _coerce_local_floating_dt(value: Any) -> datetime:
    """Return a local, timezone-naive datetime for Local Calendar storage."""
    return _coerce_dt(value).replace(tzinfo=None)


def _coerce_date(value: Any) -> date:
    """Accept a date/datetime object or string and return a date."""
    if isinstance(value, datetime):
        return _coerce_dt(value).date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    d = dt_util.parse_date(s)
    if d:
        return d
    return _to_dt(s.replace(" ", "T")).date()


def _coerce_all_day_end(value: Any, start: date) -> date:
    """Return an exclusive all-day event end date."""
    end = _coerce_date(value)
    if end < start:
        raise ServiceValidationError("end must be on or after start for all-day events.")
    if end == start:
        return end + timedelta(days=1)
    return end


def _normalize_summary(s: str | None) -> str | None:
    if s is None:
        return None
    s2 = " ".join(str(s).strip().split())
    return s2.lower()


def _uid_from_call_data(data: Mapping[str, Any]) -> str | None:
    """Accept UID from several common keys used by cards/integrations."""
    for key in UID_DATA_KEYS:
        value = data.get(key)
        if value:
            return str(value).strip()
    return None


def _ical_property_value(value: Any) -> Any:
    """Return raw value from iCalendar property wrapper."""
    return getattr(value, "dt", value)


def _dt_from_ical(value: Any) -> datetime | None:
    """Convert DTSTART/DTEND's .dt (date or datetime) to datetime (local)."""
    if value is None:
        return None
    raw = _ical_property_value(value)
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
        return dt_util.as_local(raw)
    try:
        return dt_util.as_local(dt_util.start_of_local_day(raw))
    except Exception:
        return None


def _event_end_dt(component: Any) -> datetime | None:
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
            if isinstance(dur.dt, timedelta):
                return start + dur.dt
        except Exception:
            return None
    return None


def _iso_ical_value(value: Any) -> str | None:
    """Convert iCalendar date/datetime values to ISO strings."""
    if value is None:
        return None
    raw = _ical_property_value(value)
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            raw = raw.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
        return dt_util.as_local(raw).isoformat()
    if isinstance(raw, date):
        return raw.isoformat()
    return str(raw)


def _is_all_day_component(component: Any) -> bool:
    start_raw = _ical_property_value(component.get("DTSTART"))
    return isinstance(start_raw, date) and not isinstance(start_raw, datetime)


def _match_event(
    component: Any,
    uid: str | None,
    summary: str | None,
    start: datetime | None,
    end: datetime | None,
) -> bool:
    if component.name != "VEVENT":
        return False

    if uid:
        ev_uid = str(component.get("UID", "")).strip()
        return ev_uid == uid

    if summary is not None and _normalize_summary(
        str(component.get("SUMMARY", ""))
    ) != _normalize_summary(summary):
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


def _component_tzid(component: Any) -> str | None:
    tzid = component.get("TZID")
    if not tzid:
        return None
    tzid_str = str(tzid).strip()
    return tzid_str or None
