from __future__ import annotations

from homeassistant import config_entries
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ics_calendar_tools.const import DOMAIN


async def test_user_flow_creates_entry(hass: HomeAssistant) -> None:
    """Test the user flow creates the singleton service integration entry."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_USER},
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "ICS Calendar Tools"
    assert result["data"] == {}


async def test_user_flow_aborts_when_entry_exists(hass: HomeAssistant) -> None:
    """Test only one config entry can be created."""
    MockConfigEntry(domain=DOMAIN, data={}).add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_USER},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "single_instance_allowed"
