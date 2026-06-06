from __future__ import annotations

from datetime import date, datetime

import voluptuous as vol
from homeassistant.helpers import config_validation as cv

from .helpers import _non_empty_string

DATE_OR_DATETIME_OR_STRING = vol.Any(datetime, date, cv.string)

LIST_EVENTS_SCHEMA = vol.Schema(
    {
        vol.Required("calendar"): cv.entity_id,
        vol.Optional("start"): DATE_OR_DATETIME_OR_STRING,
        vol.Optional("end"): DATE_OR_DATETIME_OR_STRING,
        vol.Optional("limit"): vol.All(vol.Coerce(int), vol.Range(min=1, max=1000)),
    }
)

ADD_EVENT_SCHEMA = vol.Schema(
    {
        vol.Required("calendar"): cv.entity_id,
        vol.Required("summary"): _non_empty_string,
        vol.Required("all_day"): cv.boolean,
        vol.Required("start"): DATE_OR_DATETIME_OR_STRING,
        vol.Required("end"): DATE_OR_DATETIME_OR_STRING,
        vol.Optional("description"): cv.string,
        vol.Optional("location"): cv.string,
        vol.Optional("rrule"): cv.string,
    }
)

DELETE_EVENT_SCHEMA = vol.Schema(
    {
        vol.Required("calendar"): cv.entity_id,
        vol.Optional("uid"): cv.string,
        vol.Optional("UID"): cv.string,
        vol.Optional("id"): cv.string,
        vol.Optional("event_id"): cv.string,
        vol.Optional("eventId"): cv.string,
        vol.Optional("event_uid"): cv.string,
        vol.Optional("eventUid"): cv.string,
        vol.Optional("ical_uid"): cv.string,
        vol.Optional("icalUid"): cv.string,
        vol.Optional("summary"): _non_empty_string,
        vol.Optional("start"): DATE_OR_DATETIME_OR_STRING,
        vol.Optional("end"): DATE_OR_DATETIME_OR_STRING,
    }
)

UPDATE_EVENT_SCHEMA = vol.Schema(
    {
        vol.Required("calendar"): cv.entity_id,
        vol.Optional("uid"): cv.string,
        vol.Optional("UID"): cv.string,
        vol.Optional("id"): cv.string,
        vol.Optional("event_id"): cv.string,
        vol.Optional("eventId"): cv.string,
        vol.Optional("event_uid"): cv.string,
        vol.Optional("eventUid"): cv.string,
        vol.Optional("ical_uid"): cv.string,
        vol.Optional("icalUid"): cv.string,
        vol.Optional("summary"): _non_empty_string,
        vol.Optional("start"): DATE_OR_DATETIME_OR_STRING,
        vol.Optional("end"): DATE_OR_DATETIME_OR_STRING,
        vol.Optional("location"): cv.string,
        vol.Optional("description"): cv.string,
        vol.Optional("rrule"): cv.string,
    }
)

IMPORT_EVENTS_SCHEMA = vol.Schema(
    {
        vol.Required("calendar"): cv.entity_id,
        vol.Required("ics"): vol.All(cv.string, vol.Length(min=1)),
        vol.Optional("clear_existing_events", default=False): cv.boolean,
    }
)
