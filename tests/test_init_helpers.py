from __future__ import annotations

from datetime import date

import pytest
import voluptuous as vol
from homeassistant.exceptions import ServiceValidationError

from custom_components.ics_calendar_tools import helpers


def test_non_empty_string_rejects_whitespace() -> None:
    with pytest.raises(vol.Invalid):
        helpers._non_empty_string("   ")


def test_non_empty_string_strips_valid_text() -> None:
    assert helpers._non_empty_string("  Holiday  ") == "Holiday"


def test_uid_from_call_data_accepts_aliases() -> None:
    assert helpers._uid_from_call_data({"eventId": "abc-123"}) == "abc-123"


def test_coerce_all_day_end_same_day_is_exclusive() -> None:
    assert helpers._coerce_all_day_end("2026-05-20", date(2026, 5, 20)) == date(2026, 5, 21)


def test_coerce_all_day_end_rejects_before_start() -> None:
    with pytest.raises(ServiceValidationError):
        helpers._coerce_all_day_end("2026-05-19", date(2026, 5, 20))
