import json
import logging
import random
import time
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd
import requests

from pipelines.http import HttpClient, HttpSettings

logger = logging.getLogger(__name__)

# Internal Google ID for Mbappe related requests
TOPIC_ID = "/g/11bx55_6wp"

GEOGRAPHIES = {
    "global": "",
    "fr": "FR",
    "es": "ES",
}

BASE_URL = "https://trends.google.com"
HOME_URL = f"{BASE_URL}/home"
EXPLORE_API_URL = f"{BASE_URL}/trends/api/explore"
MULTILINE_API_URL = f"{BASE_URL}/trends/api/widgetdata/multiline"

REQUEST_DELAY_RANGE_SEC = (5.0, 10.0)

COMMON_HEADERS = {
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
}

API_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "referer": HOME_URL,
}

OUTPUT_COLUMNS = [
    "week_start",
    "audience_scope",
    "interest",
]


class GoogleTrendsBlockedError(RuntimeError):
    pass


def strip_xssi_prefix(response_text: str) -> str:
    if response_text.startswith(")]}'"):
        return response_text.split("\n", 1)[1]
    return response_text


def parse_json_response(response: requests.Response) -> dict[str, Any]:
    payload = json.loads(strip_xssi_prefix(response.text))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object from {response.url}")
    return payload


def log_response(label: str, response: requests.Response) -> None:
    retry_history = response.raw.retries.history
    logger.info(
        f"Google Trends {label}: status={response.status_code}, "
        f"elapsed={response.elapsed.total_seconds():.2f}s, "
        f"retries={len(retry_history)}, base url={response.url.split('?', 1)[0]}"
    )

    for index, retry in enumerate(retry_history, start=1):
        logger.warning(
            f"Google Trends {label} retry {index}: "
            f"status={retry.status}, error={retry.error}"
        )


def raise_if_google_blocked(response: requests.Response) -> None:
    if "google.com/sorry/" in response.url:
        raise GoogleTrendsBlockedError(
            "Google Trends blocked the request and redirected to google.com/sorry. "
            "Stop the run and retry later."
        )


def get_checked_response(
    client: HttpClient,
    url: str,
    label: str,
    **kwargs: Any,
) -> requests.Response:
    response = client.session.get(url, timeout=client.timeout, **kwargs)
    log_response(label, response)
    raise_if_google_blocked(response)
    response.raise_for_status()
    return response


def sleep_between_requests() -> None:
    delay = random.uniform(*REQUEST_DELAY_RANGE_SEC)
    logger.debug(f"Sleep {delay:.2f}s before next request")
    time.sleep(delay)


def build_client() -> HttpClient:
    client = HttpClient(settings=HttpSettings(retry_total=3, retry_backoff_factor=6))
    client.session.headers.update(COMMON_HEADERS)

    get_checked_response(
        client,
        HOME_URL,
        "home",
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )

    return client


def build_explore_request(
    geo_code: str, start_date: date, end_date: date
) -> dict[str, Any]:
    return {
        "comparisonItem": [
            {
                "keyword": TOPIC_ID,
                "geo": geo_code,
                "time": f"{start_date.isoformat()} {end_date.isoformat()}",
            }
        ],
        "category": 0,
        "property": "",
    }


def fetch_timeseries_widget(
    client: HttpClient,
    geo_code: str,
    start_date: date,
    end_date: date,
) -> tuple[dict[str, Any], str]:
    response = get_checked_response(
        client,
        EXPLORE_API_URL,
        f"explore geo={geo_code or 'global'} window={start_date}..{end_date}",
        params={
            "hl": "en-US",
            "tz": 0,
            "req": json.dumps(
                build_explore_request(geo_code, start_date, end_date),
                separators=(",", ":"),
            ),
        },
        headers=API_HEADERS,
    )
    payload = parse_json_response(response)

    widgets = payload.get("widgets", [])
    timeseries_widget = next(
        (widget for widget in widgets if widget.get("id") == "TIMESERIES"),
        None,
    )
    if timeseries_widget is None:
        raise ValueError("Google Trends response does not contain TIMESERIES widget")

    request = timeseries_widget["request"]
    logger.info(f"Google Trends resolution: {request.get('resolution')!r}")

    return request, timeseries_widget["token"]


def fetch_timeline(
    client: HttpClient,
    request: dict[str, Any],
    token: str,
) -> dict[str, Any]:
    response = get_checked_response(
        client,
        MULTILINE_API_URL,
        "timeline",
        params={
            "hl": "en-US",
            "tz": 0,
            "req": json.dumps(request, separators=(",", ":")),
            "token": token,
        },
        headers=API_HEADERS,
    )
    return parse_json_response(response)


def parse_timeline(payload: dict[str, Any]) -> pd.DataFrame:
    timeline = payload.get("default", {}).get("timelineData")
    if not isinstance(timeline, list):
        raise ValueError("Google Trends response does not contain timelineData")

    records = []
    for point in timeline:
        values = point["value"]
        records.append(
            {
                "period_start": datetime.fromtimestamp(
                    int(point["time"]), UTC
                ).date(),
                "interest": int(values[0]),
            }
        )

    frame = pd.DataFrame.from_records(records, columns=["period_start", "interest"])
    frame["interest"] = frame["interest"].astype("int64")
    return frame


def fetch_timeline_frame(
    client: HttpClient,
    geo_code: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    widget_request, token = fetch_timeseries_widget(
        client=client,
        geo_code=geo_code,
        start_date=start_date,
        end_date=end_date,
    )
    resolution = widget_request.get("resolution")
    if resolution != "WEEK":
        raise ValueError(f"Expected weekly Google Trends data, got {resolution!r}")

    sleep_between_requests()

    payload = fetch_timeline(
        client=client,
        request=widget_request,
        token=token,
    )
    frame = parse_timeline(payload)
    if not frame["period_start"].map(is_sunday).all():
        raise ValueError("Expected Sunday-start weekly Google Trends data")
    return (
        frame.rename(columns={"period_start": "week_start"})
        .sort_values("week_start")
        .reset_index(drop=True)
    )


def is_sunday(value: date) -> bool:
    return value.weekday() == 6


def fetch_geography(
    client: HttpClient,
    geo_name: str,
    geo_code: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    logger.info(f"Fetch weekly timeline for {geo_name} region")

    frame = fetch_timeline_frame(
        client=client,
        geo_code=geo_code,
        start_date=start_date,
        end_date=end_date,
    )

    logger.info(f"{geo_name} region: {len(frame)} weekly points")
    return pd.DataFrame(frame, columns=["week_start", "interest"])


def fetch_all_geographies(start_date: date, end_date: date) -> pd.DataFrame:
    with build_client() as client:
        all_geographies_dfs = []

        for index, (geo_name, geo_code) in enumerate(GEOGRAPHIES.items()):
            geography_df = fetch_geography(
                client=client,
                geo_name=geo_name,
                geo_code=geo_code,
                start_date=start_date,
                end_date=end_date,
            )
            geography_df["audience_scope"] = geo_name
            all_geographies_dfs.append(geography_df)

            if index < len(GEOGRAPHIES) - 1:
                sleep_between_requests()

    frame = pd.concat(all_geographies_dfs, ignore_index=True)

    return pd.DataFrame(frame, columns=OUTPUT_COLUMNS)
