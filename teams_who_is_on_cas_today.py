import logging
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pytz
import requests


UK_TZ = pytz.timezone("Europe/London")

# Kept hard-coded intentionally, per current requirement.
POWER_AUTOMATE_WEBHOOK = "https://default5668021c34de4215a3e77791e4b6d5.04.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/15ad2b69b8ce4d54a6d50a42755f5b5b/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=HVURVit5ytt-9bzxUKz9TG3aHWjeVKixV2Mcr__8FEc"

AUTH_URL = "https://www.humanity.com/oauth2/token.php"
SHIFTS_URL = "https://www.humanity.com/api/v2/shifts"
REQUEST_TIMEOUT_SECONDS = 30

HUMANITY_ENV_VARS = {
    "client_id": "HUMANITY_CLIENT_ID",
    "client_secret": "HUMANITY_CLIENT_SECRET",
    "username": "HUMANITY_USERNAME",
    "password": "HUMANITY_PASSWORD",
}


@dataclass(frozen=True)
class TeamConfig:
    team_name: str
    locations: Dict[str, str]
    window_start_hour: int
    window_end_hour: int
    cutoff_hour: int
    send_empty_card: bool = True


@dataclass(frozen=True)
class TeamShift:
    employee_name: str
    display_name: str
    role: str
    start_time: str
    end_time: str
    location_id: str
    raw_shift: dict


CAS_ONLY_TEAM = TeamConfig(
    team_name="Who is on CAS Today",
    locations={
        "936220": "CAS",  # Only CAS
    },
    window_start_hour=7,
    window_end_hour=19,
    cutoff_hour=20,
    send_empty_card=True,
)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


logger = logging.getLogger(__name__)


def get_humanity_credentials() -> Dict[str, str]:
    creds = {key: os.getenv(env_name) for key, env_name in HUMANITY_ENV_VARS.items()}
    missing = [env_name for env_name in HUMANITY_ENV_VARS.values() if not os.getenv(env_name)]
    if missing:
        raise ValueError(f"Missing required Humanity environment variables: {', '.join(missing)}")
    return creds


def safe_first_employee(shift: dict) -> Optional[dict]:
    employees = shift.get("employees") or []
    if not employees:
        return None
    return employees[0] or None


def normalize_employee_name(employee: Optional[dict]) -> str:
    if not employee:
        return ""
    return (employee.get("name") or "").strip().title()


def get_first_name(full_name: str) -> str:
    parts = full_name.split()
    return parts[0] if parts else "Unknown"


def parse_shift_datetime(shift_date: dict, default_date=None) -> datetime:
    if default_date is None:
        default_date = datetime.now(UK_TZ).date()

    date_str = shift_date.get("date")
    time_str = shift_date.get("time", "00:00") or "00:00"

    try:
        hour, minute = map(int, time_str.split(":")[:2])
    except Exception:
        hour, minute = 0, 0

    if date_str:
        try:
            date_part = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            date_part = default_date
    else:
        date_part = default_date

    naive_dt = datetime.combine(date_part, datetime.min.time()) + timedelta(hours=hour, minutes=minute)
    return UK_TZ.localize(naive_dt)


def get_role_for_shift(shift: dict, team_config: TeamConfig) -> Optional[str]:
    location_id = str(shift.get("schedule_location_id", "") or "")
    return team_config.locations.get(location_id)


def is_shift_start_in_window(shift: dict, team_config: TeamConfig) -> bool:
    start_dt = parse_shift_datetime(shift.get("start_date", {}))
    start_hour_minute = (start_dt.hour, start_dt.minute)
    window_start = (team_config.window_start_hour, 0)
    window_end = (team_config.window_end_hour, 0)
    return window_start <= start_hour_minute <= window_end


def filter_team_shifts_for_today(
    shifts: List[dict],
    team_config: TeamConfig,
    now: Optional[datetime] = None,
) -> List[TeamShift]:
    if now is None:
        now = datetime.now(UK_TZ)

    team_shifts: List[TeamShift] = []
    logger.info(f"🔍 UK time: {now.strftime('%H:%M:%S')} | Found {len(shifts)} approved shifts today...")

    for shift in shifts:
        location_id = str(shift.get("schedule_location_id", "") or "")
        role = get_role_for_shift(shift, team_config)

        if not role:
            schedule_name = shift.get("schedule_name", "") or ""
            employee_name = normalize_employee_name(safe_first_employee(shift)) or "No Employee"
            logger.info(f"   {schedule_name[:15]:<15} - {employee_name}")
            continue

        employee = safe_first_employee(shift)
        employee_name = normalize_employee_name(employee)
        if not employee_name:
            continue

        start_time = shift.get("start_date", {}).get("time", "N/A")
        end_time = shift.get("end_date", {}).get("time", "N/A")

        if not is_shift_start_in_window(shift, team_config):
            logger.info(f"⏭️ {role} OUTSIDE WINDOW: {employee_name} ({start_time}-{end_time}) | loc:{location_id}")
            continue

        team_shifts.append(
            TeamShift(
                employee_name=employee_name,
                display_name=get_first_name(employee_name),
                role=role,
                start_time=start_time,
                end_time=end_time,
                location_id=location_id,
                raw_shift=shift,
            )
        )

        logger.info(f"✅ {role} INCLUDED: {employee_name} ({start_time}-{end_time}) | loc:{location_id}")

    team_shifts.sort(key=lambda s: (s.role, s.start_time, s.display_name))
    logger.info(f"✅ Found {len(team_shifts)} matching daytime shifts for {team_config.team_name}")
    return team_shifts


def get_humanity_token() -> str:
    creds = get_humanity_credentials()

    auth_data = {
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "grant_type": "password",
        "username": creds["username"],
        "password": creds["password"],
    }

    response = requests.post(AUTH_URL, data=auth_data, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    data = response.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Humanity token response did not include access_token")
    return token


def get_todays_approved_shifts(token: str, now: Optional[datetime] = None) -> List[dict]:
    if now is None:
        now = datetime.now(UK_TZ)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    today = now.strftime("%Y-%m-%d")
    params = {"start_date": today, "end_date": today, "status": "approved"}

    response = requests.get(SHIFTS_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    data = response.json()
    return data.get("data", [])


def add_section_header(body: List[dict], title: str) -> None:
    body.append(
        {
            "type": "TextBlock",
            "text": title,
            "weight": "Bolder",
            "spacing": "Medium",
            "wrap": True,
        }
    )


def add_shift_lines(body: List[dict], shifts: List[TeamShift]) -> None:
    for shift in shifts:
        body.append(
            {
                "type": "TextBlock",
                "text": f"• {shift.display_name} ({shift.start_time}-{shift.end_time})",
                "spacing": "Small",
                "wrap": True,
            }
        )


def build_adaptive_card_payload(team_shifts: List[TeamShift], team_config: TeamConfig) -> dict:
    now = datetime.now(UK_TZ)
    today_date = now.strftime("%A, %d %B %Y")

    body = [
        {
            "type": "TextBlock",
            "text": f"👥 {team_config.team_name}",
            "size": "Large",
            "weight": "Bolder",
            "color": "Attention",
        },
        {
            "type": "TextBlock",
            "text": today_date,
            "weight": "Bolder",
            "spacing": "Small",
        },
    ]

    if not team_shifts:
        body.append(
            {
                "type": "TextBlock",
                "text": "No CAS shifts found today.",
                "spacing": "Small",
                "wrap": True,
            }
        )
    else:
        if team_shifts:
            add_section_header(body, "CAS")
            add_shift_lines(body, team_shifts)

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": body,
                    "actions": [],
                },
            }
        ],
    }


def send_powerautomate_card(webhook_url: str, payload: dict) -> bool:
    logger.info(f"🔗 Sending Adaptive Card to Power Automate: {webhook_url[:60]}...")

    response = requests.post(
        webhook_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    logger.info(f"🌐 Power Automate response: {response.status_code} - {response.text[:200]}")
    response.raise_for_status()
    return response.status_code in (200, 202)


def should_run_now(team_config: TeamConfig, now: Optional[datetime] = None) -> bool:
    if now is None:
        now = datetime.now(UK_TZ)

    cutoff = now.replace(hour=team_config.cutoff_hour, minute=0, second=0, microsecond=0)
    return now < cutoff


def run_shift_check(team_config: TeamConfig, dry_run: bool = False) -> int:
    now_uk = datetime.now(UK_TZ)

    logger.info(f"🕐 Local time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🕐 UK time: {now_uk.strftime('%Y-%m-%d %H:%M:%S')}")

    if not should_run_now(team_config, now_uk):
        logger.info(
            f"ℹ️ Outside allowed window for {team_config.team_name} "
            f"(before {team_config.cutoff_hour}:00 UK). Exiting."
        )
        return 0

    logger.info(
        f"🚀 Running shift check for {team_config.team_name} "
        f"at {now_uk.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    token = get_humanity_token()
    shifts = get_todays_approved_shifts(token, now_uk)
    team_shifts = filter_team_shifts_for_today(shifts, team_config, now_uk)

    cas_count = sum(1 for s in team_shifts)
    logger.info(f"📊 {team_config.team_name}: total CAS = {cas_count}")

    payload = build_adaptive_card_payload(team_shifts, team_config)

    if dry_run:
        logger.info("🧪 Dry run enabled - card payload built but not posted")
        return 0

    if send_powerautomate_card(POWER_AUTOMATE_WEBHOOK, payload):
        logger.info(f"✅ Posted to Power Automate for {team_config.team_name}")
        return 0

    logger.error("❌ Failed to post to Power Automate")
    return 1


if __name__ == "__main__":
    configure_logging()
    dry_run = "--dry-run" in sys.argv
    try:
        sys.exit(run_shift_check(CAS_ONLY_TEAM, dry_run=dry_run))
    except Exception as exc:
        logger.error(f"❌ Error: {exc}")
        traceback.print_exc()
        sys.exit(1)
