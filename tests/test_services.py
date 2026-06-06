from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, cast

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import entity_registry as er
from icalendar import Calendar
from pytest_homeassistant_custom_component.common import MockConfigEntry

import custom_components.ics_calendar_tools as services
from custom_components.ics_calendar_tools.const import DOMAIN

LOCAL_CALENDAR_DOMAIN = "local_calendar"
CALENDAR_ENTITY_ID = "calendar.family"


def _write_calendar(path: Path, *components: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "BEGIN:VCALENDAR",
                "VERSION:2.0",
                "PRODID:-//ics-calendar-tools tests//EN",
                *components,
                "END:VCALENDAR",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _event(
    uid: str,
    summary: str,
    *,
    start: str = "DTSTART:20260520T090000",
    end: str = "DTEND:20260520T100000",
    extra: tuple[str, ...] = (),
) -> str:
    return "\n".join(
        [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            start,
            end,
            f"SUMMARY:{summary}",
            *extra,
            "END:VEVENT",
        ]
    )


def _calendar_from_path(path: Path) -> Calendar:
    return Calendar.from_ical(path.read_bytes())


def _events(path: Path) -> list[Any]:
    return list(_calendar_from_path(path).walk("VEVENT"))


def _event_by_uid(path: Path, uid: str) -> Any:
    for event in _events(path):
        if str(event.get("UID")) == uid:
            return event
    raise AssertionError(f"Event {uid} not found in {path}")


def _local_calendar_path(hass: HomeAssistant, storage_key: str) -> Path:
    return Path(hass.config.path(".storage", f"local_calendar.{storage_key}.ics"))


def _add_local_calendar(
    hass: HomeAssistant,
    storage_key: str = "family",
    entity_id: str = CALENDAR_ENTITY_ID,
) -> Path:
    entry = MockConfigEntry(
        domain=LOCAL_CALENDAR_DOMAIN,
        title=storage_key.title(),
        data={"storage_key": storage_key},
    )
    entry.add_to_hass(hass)

    registry = er.async_get(hass)
    registry.async_get_or_create(
        "calendar",
        LOCAL_CALENDAR_DOMAIN,
        storage_key,
        suggested_object_id=entity_id.split(".", 1)[1],
        config_entry=entry,
        original_name=storage_key.title(),
    )

    path = _local_calendar_path(hass, storage_key)
    _write_calendar(path)
    return path


@pytest.fixture
async def setup_integration(hass: HomeAssistant) -> None:
    """Set up the custom integration through config entry only."""
    entry = MockConfigEntry(domain=DOMAIN, data={})
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()


async def test_list_events_returns_response(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Test list_events returns sorted event data through the service registry."""
    path = _add_local_calendar(hass)
    _write_calendar(
        path,
        _event(
            "timed",
            "Timed event",
            start="DTSTART:20260521T090000",
            end="DTEND:20260521T100000",
        ),
        _event(
            "all-day",
            "All day event",
            start="DTSTART;VALUE=DATE:20260520",
            end="DTEND;VALUE=DATE:20260521",
        ),
    )

    response = cast(
        dict[str, Any],
        await hass.services.async_call(
            DOMAIN,
            "list_events",
            {"calendar": CALENDAR_ENTITY_ID, "limit": 10},
            blocking=True,
            return_response=True,
        ),
    )
    events = cast(list[dict[str, Any]], response["events"])

    assert response["calendar"] == CALENDAR_ENTITY_ID
    assert response["count"] == 2
    assert [event["uid"] for event in events] == ["all-day", "timed"]
    assert events[0]["all_day"] is True
    assert events[0]["start"] == "2026-05-20"


async def test_add_event_writes_all_day_event_with_rrule(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Test add_event writes an all-day event with exclusive DTEND and RRULE."""
    path = _add_local_calendar(hass)

    await hass.services.async_call(
        DOMAIN,
        "add_event",
        {
            "calendar": CALENDAR_ENTITY_ID,
            "summary": "Holiday",
            "all_day": True,
            "start": "2026-05-20",
            "end": "2026-05-20",
            "rrule": "FREQ=DAILY;COUNT=2",
        },
        blocking=True,
    )

    event = next(event for event in _events(path) if str(event.get("SUMMARY")) == "Holiday")
    assert event.get("DTSTART").dt == date(2026, 5, 20)
    assert event.get("DTEND").dt == date(2026, 5, 21)
    assert event.get("RRULE")["FREQ"] == ["DAILY"]


async def test_update_event_preserves_all_day_dates(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Test update_event keeps all-day events as date values."""
    path = _add_local_calendar(hass)
    _write_calendar(
        path,
        _event(
            "all-day",
            "Original",
            start="DTSTART;VALUE=DATE:20260520",
            end="DTEND;VALUE=DATE:20260521",
        ),
    )

    await hass.services.async_call(
        DOMAIN,
        "update_event",
        {
            "calendar": CALENDAR_ENTITY_ID,
            "uid": "all-day",
            "summary": "Updated",
            "start": "2026-05-22",
            "end": "2026-05-22",
        },
        blocking=True,
    )

    event = _event_by_uid(path, "all-day")
    assert str(event.get("SUMMARY")) == "Updated"
    assert event.get("DTSTART").dt == date(2026, 5, 22)
    assert event.get("DTEND").dt == date(2026, 5, 23)


async def test_update_event_requires_change(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Test update_event rejects a no-op update."""
    path = _add_local_calendar(hass)
    _write_calendar(path, _event("one", "One"))

    with pytest.raises(ServiceValidationError):
        await hass.services.async_call(
            DOMAIN,
            "update_event",
            {"calendar": CALENDAR_ENTITY_ID, "uid": "one"},
            blocking=True,
        )


async def test_delete_event_only_deletes_from_selected_calendar(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Test delete_event does not delete a UID from another Local Calendar."""
    family_path = _add_local_calendar(hass, "family", "calendar.family")
    work_path = _add_local_calendar(hass, "work", "calendar.work")
    _write_calendar(family_path, _event("family-only", "Family"))
    _write_calendar(work_path, _event("shared-uid", "Work"))

    with pytest.raises(ServiceValidationError):
        await hass.services.async_call(
            DOMAIN,
            "delete_event",
            {"calendar": "calendar.family", "uid": "shared-uid"},
            blocking=True,
        )

    assert str(_event_by_uid(work_path, "shared-uid").get("SUMMARY")) == "Work"
    assert str(_event_by_uid(family_path, "family-only").get("SUMMARY")) == "Family"


async def test_delete_event_requires_exact_uid_match(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Delete must not match by UID substring; only exact UID is valid."""
    path = _add_local_calendar(hass)
    _write_calendar(path, _event("uid-123456789012345", "Exact UID only"))

    with pytest.raises(ServiceValidationError):
        await hass.services.async_call(
            DOMAIN,
            "delete_event",
            {
                "calendar": CALENDAR_ENTITY_ID,
                "uid": "123456789012",
            },
            blocking=True,
        )

    assert str(_event_by_uid(path, "uid-123456789012345").get("SUMMARY")) == "Exact UID only"


async def test_import_events_rejects_duplicate_uid_without_writing(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Test import_events rejects duplicate UIDs in the selected calendar."""
    path = _add_local_calendar(hass)
    _write_calendar(path, _event("existing", "Existing"))

    with pytest.raises(ServiceValidationError):
        await hass.services.async_call(
            DOMAIN,
            "import_events",
            {
                "calendar": CALENDAR_ENTITY_ID,
                "ics": "\n".join(
                    [
                        "BEGIN:VCALENDAR",
                        "VERSION:2.0",
                        _event("existing", "Duplicate"),
                        "END:VCALENDAR",
                    ]
                ),
            },
            blocking=True,
        )

    assert [str(event.get("UID")) for event in _events(path)] == ["existing"]


async def test_import_events_clear_existing_events_replaces_events(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Test import_events can replace the selected calendar contents."""
    path = _add_local_calendar(hass)
    _write_calendar(path, _event("old", "Old"))

    await hass.services.async_call(
        DOMAIN,
        "import_events",
        {
            "calendar": CALENDAR_ENTITY_ID,
            "clear_existing_events": True,
            "ics": "\n".join(
                [
                    "BEGIN:VCALENDAR",
                    "VERSION:2.0",
                    _event("new", "New"),
                    "END:VCALENDAR",
                ]
            ),
        },
        blocking=True,
    )

    assert [str(event.get("UID")) for event in _events(path)] == ["new"]


async def test_find_ics_path_uses_storage_key_not_entity_object_id(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Resolve the file from config_entry storage_key, not from entity object_id."""
    # Intentionally mismatch storage_key and object_id to catch path resolution regressions.
    path = _add_local_calendar(
        hass,
        storage_key="storage-slug",
        entity_id="calendar.pretty_name",
    )

    resolved = await services._find_ics_path_for_calendar(hass, "calendar.pretty_name")

    assert Path(resolved) == path
    assert Path(resolved).name == "local_calendar.storage-slug.ics"


async def test_local_calendar_path_is_under_ha_config_dir(
    hass: HomeAssistant,
    setup_integration: None,
) -> None:
    """Build .ics path relative to Home Assistant config directory."""
    resolved = Path(services._local_calendar_ics_path(hass, "family"))
    expected = Path(hass.config.path(".storage", "local_calendar.family.ics"))

    assert resolved == expected
    assert str(resolved).startswith(hass.config.config_dir)


async def test_import_all_day_event_with_time_zero(
    hass: HomeAssistant, setup_integration: None
) -> None:
    """Import event with DTSTART/DTEND as date+T000000 and treat as all-day."""
    path = _add_local_calendar(hass)
    ics = """
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:allday-timezero
DTSTART:20260520T000000
DTEND:20260521T000000
SUMMARY:All-day with time zero
END:VEVENT
END:VCALENDAR
"""
    await hass.services.async_call(
        DOMAIN,
        "import_events",
        {"calendar": CALENDAR_ENTITY_ID, "ics": ics},
        blocking=True,
    )
    event = _event_by_uid(path, "allday-timezero")
    # Should be interpreted as all-day
    assert event.get("DTSTART").dt == date(2026, 5, 20)
    assert event.get("DTEND").dt == date(2026, 5, 21)


async def test_import_invalid_ics_data(hass: HomeAssistant, setup_integration: None) -> None:
    """Import should fail gracefully on invalid ICS data."""
    path = _add_local_calendar(hass)
    bad_ics = """
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:bad
DTSTART:NOTADATE
SUMMARY:Broken
END:VEVENT
END:VCALENDAR
"""
    with pytest.raises(ServiceValidationError):
        await hass.services.async_call(
            DOMAIN,
            "import_events",
            {"calendar": CALENDAR_ENTITY_ID, "ics": bad_ics},
            blocking=True,
        )
    # Calendar should remain empty
    assert _events(path) == []


async def test_import_large_ics_file(hass: HomeAssistant, setup_integration: None) -> None:
    """Import a large ICS file and check stability and count."""
    path = _add_local_calendar(hass)
    events = [
        f"""BEGIN:VEVENT\nUID:bulk{i}\nDTSTART:202605{i % 28 + 1:02d}T120000\n"
        f"DTEND:202605{i % 28 + 1:02d}T130000\nSUMMARY:Event {i}\nEND:VEVENT"""
        for i in range(1000)
    ]
    ics = "\n".join(
        [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            *events,
            "END:VCALENDAR",
        ]
    )
    await hass.services.async_call(
        DOMAIN,
        "import_events",
        {"calendar": CALENDAR_ENTITY_ID, "ics": ics},
        blocking=True,
    )
    assert len(_events(path)) == 1000
