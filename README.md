# ICS Calendar Tools (Home Assistant)

**ICS Calendar Tools** is a Home Assistant custom integration that lets you **add, edit, and delete events** in **Local Calendar (.ics)** calendars by writing directly to the calendar’s `.ics` file and then reloading Local Calendar so changes appear immediately.

This was built to work well with dashboards like **Week Planner Card Plus** (Skylight-style family calendar dashboards).

---

## Features

- ✅ Add events to a Local Calendar (`.ics`)
- ✅ Edit events (update title/time/details)
- ✅ Delete events reliably (UID-based when available)
- ✅ Refreshes Local Calendar after changes (no manual restart required)
- ✅ Works with multiple Local Calendar entities

---

## Requirements

- Home Assistant with **Local Calendar** configured.
- A calendar entity created by Local Calendar (e.g. `calendar.family_calendar`).

---

## Installation (HACS)

1. Open **HACS** → **Integrations**
2. Click the 3 dots (top right) → **Custom repositories**
3. Add this repository URL "https://github.com/randrcomputers/ics-calendar-tools.git" and select category **Integration**
4. Install **ICS Calendar Tools**
5. Restart Home Assistant
6. install ics calendar tools from Devices and services Add integration "ics Calendar Tools"


> After that, you should see the integration’s services available under Developer Tools → Services.
> ics_calendar_tools.delete_event
> ics_calendar_tools.update_event





