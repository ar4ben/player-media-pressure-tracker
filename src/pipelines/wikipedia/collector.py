from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any
from urllib.parse import quote

import pandas as pd

import pipelines.date_range as date_range
from pipelines.http import HttpClient, HttpSettings

logger = logging.getLogger(__name__)

ARTICLE_TITLE = "Kylian_Mbappé"
LANGUAGES = ("en", "fr", "es")

ANALYTICS_API_URL = "https://wikimedia.org/api/rest_v1/metrics"
REQUEST_DELAY_SEC = 1.0

HEADERS = {
    "accept": "application/json",
    "user-agent": "media-pressure/0.1 requests",
}

OUTPUT_COLUMNS = [
    "date",
    "language",
    "article_url",
    "views",
]


def build_article_url(language: str) -> str:
    article = quote(ARTICLE_TITLE)
    return f"https://{language}.wikipedia.org/wiki/{article}"


def build_views_url(project: str, start_date: date, end_date: date) -> str:
    article = quote(ARTICLE_TITLE)
    start = start_date.strftime("%Y%m%d00")
    end = end_date.strftime("%Y%m%d00")
    return (
        f"{ANALYTICS_API_URL}/pageviews/per-article/"
        f"{project}/all-access/user/{article}/daily/{start}/{end}"
    )


def fetch_json(client: HttpClient, url: str) -> dict[str, Any]:
    response = client.session.get(url, timeout=client.timeout)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object from {url}")
    return payload


def parse_views(payload: dict[str, Any]) -> pd.DataFrame:
    items = payload.get("items")
    if not isinstance(items, list):
        raise ValueError("Pageviews response does not contain an items list")

    records: list[dict[str, Any]] = [
        {
            "date": datetime.strptime(item["timestamp"], "%Y%m%d%H").date(),
            "views": item["views"],
        }
        for item in items
    ]

    frame = pd.DataFrame.from_records(records, columns=["date", "views"])
    frame["views"] = frame["views"].astype("int64")
    return frame


def fetch_language(
    client: HttpClient,
    language: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    project = f"{language}.wikipedia.org"

    views_payload = fetch_json(
        client,
        build_views_url(project, start_date, end_date),
    )
    views = parse_views(views_payload)

    calendar = pd.DataFrame(
        {"date": list(date_range.generate_days_from_range(start_date, end_date))}
    )
    frame = calendar.merge(views, on="date", how="left", validate="one_to_one")
    frame["views"] = frame["views"].fillna(0).astype("int64")

    frame["language"] = language
    frame["article_url"] = build_article_url(language)

    return pd.DataFrame(frame, columns=OUTPUT_COLUMNS)


def fetch_all_languages(start_date: date, end_date: date) -> pd.DataFrame:
    with HttpClient(settings=HttpSettings(read_timeout=30)) as client:
        client.session.headers.update(HEADERS)

        all_languages_dfs = []
        for index, language in enumerate(LANGUAGES):
            language_df = fetch_language(
                client=client,
                language=language,
                start_date=start_date,
                end_date=end_date,
            )
            views_total = language_df["views"].sum()
            logger.info(
                f"{language} page: "
                f"{len(language_df)} days, {views_total} total views"
            )
            all_languages_dfs.append(language_df)
            if index < len(LANGUAGES) - 1:
                time.sleep(REQUEST_DELAY_SEC)

    return pd.concat(all_languages_dfs, ignore_index=True)
