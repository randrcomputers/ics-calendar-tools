# ICS Calendar Tools

Custom integration that edits local `.ics` calendar files via services.

Folder: `custom_components/ics_calendar_tools/`

After installing, restart Home Assistant (or reload the integration if supported).

## Services

- `ics_calendar_tools.add_event` — add a new event (optionally with RRULE) to a Local Calendar `.ics` file.
- `ics_calendar_tools.update_event` — update an event by UID (now also accepts optional `rrule`).
- `ics_calendar_tools.delete_event` — delete an event by UID (deletes whole series if the UID is the master RRULE event).
