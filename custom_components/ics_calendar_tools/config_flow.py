from __future__ import annotations

from homeassistant import config_entries

from .const import DOMAIN


class ICSCalendarToolsConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Config flow for ICS Calendar Tools."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        # No settings required; this integration only registers services.
        # Creating a config entry allows the integration to load without YAML.
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        return self.async_create_entry(title="ICS Calendar Tools", data={})
