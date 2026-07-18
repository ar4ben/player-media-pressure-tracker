from __future__ import annotations

import json
import logging
import re
import time
from datetime import date
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup, Tag

from pipelines.http import HttpClient

logger = logging.getLogger(__name__)

PLAYER_SLUG = "Kylian-Mbappe-Lottin"
BASE_SITE_URL = "https://www.live-result.com"
PLAYER_PAGE_URL = f"{BASE_SITE_URL}/football/players/{PLAYER_SLUG}"

REQUEST_DELAY_SEC = 1.0

COMMON_HEADERS = {
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
}

AJAX_HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "referer": PLAYER_PAGE_URL,
    "x-requested-with": "XMLHttpRequest",
}

TIME_METRIC_MAPPINGS = {
    "total time": "total_time",
    "average time": "average_time",
}

EVENT_METRIC_MAPPINGS = {
    "goal": "goals",
    "penalty": "penalties",
    "missing penalty": "missed_penalties",
    "yellow card": "yellow_cards",
    "red card": "red_cards",
}

METRIC_NAME_BY_TITLE = TIME_METRIC_MAPPINGS | EVENT_METRIC_MAPPINGS

OUTPUT_COLUMNS = [
    "competition",
    "match_date",
    "team1",
    "score",
    "team2",
    "total_time",
    "goals",
    "penalties",
    "missed_penalties",
    "yellow_cards",
    "red_cards",
]


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def parse_number(value: str, *, metric: str, context: str) -> int | float:
    value = clean_text(value)
    if re.fullmatch(r"\d+", value):
        return int(value)
    raise ValueError(f"Unexpected numeric value {value!r} for {metric} in {context}")


def parse_date(value: str) -> date:
    match = re.search(r"(\d{4})/(\d{2})/(\d{2})", value)
    if not match:
        raise ValueError(f"Could not parse match date from {value!r}")
    year, month, day = map(int, match.groups())
    return date(year, month, day)


def get_tournaments_dict(soup: BeautifulSoup) -> dict[str, str]:
    tabs: dict[str, str] = {}
    for link in soup.select(
        'div[aria-labelledby="tournamentsMenuButton"] a[data-toggle="tab"][href^="#t"]'
    ):
        tab_id = link["href"].removeprefix("#")
        if tab_id == "tlast_matches":
            continue
        tabs[tab_id] = clean_text(link.get_text(" "))
    return tabs


def parse_metric_names(table: Tag) -> list[str]:
    columns = []
    # skip match title and match date.
    metrics_only = table.select("thead th")[2:]
    for header_cell in metrics_only:
        icon = header_cell.select_one("i[title]")
        if not icon or not icon.has_attr("title"):
            raise ValueError(f"Metric header has no title: {header_cell}")
        title = icon["title"]
        title = clean_text(title).lower()
        if title not in METRIC_NAME_BY_TITLE:
            raise ValueError(f"Unknown metric title {title!r}")
        columns.append(METRIC_NAME_BY_TITLE[title])
    return columns


def parse_match_teams_and_score(
    row: Tag,
    match_cell: Tag,
    competition: str,
) -> tuple[str, ...]:
    team_spans = match_cell.select("span.team.team1, span.team.team2")
    if len(team_spans) != 2:
        raise ValueError(
            f"Unexpected team count in {competition}: expected 2, got {len(team_spans)}; "
            f"row={clean_text(row.get_text(' ', strip=True))!r}"
        )

    team1 = clean_text(team_spans[0].get_text(" ", strip=True))
    team2 = clean_text(team_spans[1].get_text(" ", strip=True))
    score_node = match_cell.select_one("span.score")
    if not score_node:
        raise ValueError(
            f"Missing score in {competition}: row={clean_text(row.get_text(' ', strip=True))!r}"
        )
    score = clean_text(score_node.get_text(" ", strip=True))
    if not re.fullmatch(r"\d+:\d+", score):
        raise ValueError(
            f"Unexpected score {score!r} in {competition}: row={clean_text(row.get_text(' ', strip=True))!r}"
        )

    return team1, score, team2


def validate_all_metrics_zero(raw_metrics: dict[str, str], context: str) -> None:
    non_zero_events = {
        metric: raw_metrics[metric]
        for metric in EVENT_METRIC_MAPPINGS.values()
        if raw_metrics[metric] != "0"
    }
    if non_zero_events:
        raise ValueError(
            f"Time is '-' but event metrics are non-zero in {context}: {raw_metrics}"
        )


def parse_match_row(
    row: Tag,
    metrics: list[str],
    competition: str,
) -> dict[str, Any]:
    record: dict[str, Any] = {}

    cells = row.find_all("td", recursive=False)

    expected_cell_count = 2 + len(metrics)
    if len(cells) != expected_cell_count:
        raise ValueError(
            f"Unexpected cell count in {competition}: expected {expected_cell_count}, "
            f"got {len(cells)}; row={clean_text(row.get_text(' ', strip=True))!r}"
        )

    date_cell, match_cell = cells[0], cells[1]
    team1, score, team2 = parse_match_teams_and_score(
        row=row,
        match_cell=match_cell,
        competition=competition,
    )

    record.update(
        {
            "competition": competition,
            "team1": team1,
            "score": score,
            "team2": team2,
            "match_date": parse_date(date_cell.get_text(" ", strip=True)),
        }
    )

    context = f"{competition}, {record['match_date']}, {team1} {score} {team2}"
    raw_metrics = {
        metric: clean_text(cell.get_text(" ", strip=True))
        for metric, cell in zip(metrics, cells[2:], strict=True)
    }
    total_time = raw_metrics["total_time"]
    no_played_time = total_time == "-"

    if no_played_time:
        validate_all_metrics_zero(raw_metrics, context)
        record["total_time"] = None
    else:
        parsed_total_time = parse_number(
            total_time, metric="total_time", context=context
        )
        record["total_time"] = parsed_total_time

    for metric in EVENT_METRIC_MAPPINGS.values():
        record[metric] = parse_number(
            raw_metrics[metric], metric=metric, context=context
        )

    return record


def parse_api_payload(raw_payload: str) -> pd.DataFrame:
    payload = json.loads(raw_payload)
    html = payload.get("js", {}).get("result")
    if not html:
        raise ValueError("API response does not contain js.result HTML")

    soup = BeautifulSoup(html, "html.parser")
    tournaments_dict = get_tournaments_dict(soup)
    if not tournaments_dict:
        raise ValueError("No tournament tournament_tabs found in API response")

    match_records = extract_match_rows(soup=soup, tournaments_dict=tournaments_dict)

    if not match_records:
        raise ValueError("No match rows parsed from API response")

    return pd.DataFrame.from_records(match_records, columns=OUTPUT_COLUMNS)


def extract_match_rows(
    soup: BeautifulSoup,
    tournaments_dict: dict[str, str],
) -> list[dict[str, Any]]:
    match_records: list[dict[str, Any]] = []

    for pane in soup.select("div.tab-pane"):
        competition_id = pane.get("id", "")
        if competition_id not in tournaments_dict:
            continue

        table = pane.select_one("table.maintbl.standings-table")
        if not table:
            raise ValueError(
                f"Missing table for tournament tab {competition_id!r} ({tournaments_dict[competition_id]!r})"
            )

        table_body = table.select_one("tbody")
        if not table_body:
            raise ValueError(
                f"Missing table body for tournament tab {competition_id!r} ({tournaments_dict[competition_id]!r})"
            )

        metrics = parse_metric_names(table)

        for row in table_body.find_all("tr", recursive=False):
            match_records.append(
                parse_match_row(
                    row=row,
                    metrics=metrics,
                    competition=tournaments_dict[competition_id],
                )
            )

    return match_records


def fetch_year(client: HttpClient, year: int) -> str:
    response = client.session.get(
        PLAYER_PAGE_URL,
        params={
            "JsHttpRequest": "0-xml",
            "year": year,
            "do": "statistics",
            "catalogue": "en3",
        },
        headers=AJAX_HEADERS,
        timeout=client.timeout,
    )
    response.raise_for_status()
    return response.text


def build_client() -> HttpClient:
    client = HttpClient()
    client.session.headers.update(COMMON_HEADERS)

    # Get basic cookies
    response = client.session.get(
        BASE_SITE_URL,
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout=client.timeout,
    )
    response.raise_for_status()

    return client


def fetch_years(
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    with build_client() as client:
        year_frames = []
        for year in range(start_year, end_year + 1):
            api_payload = fetch_year(client, year)
            year_matches_df = parse_api_payload(api_payload)
            logger.info(f"{year} year: {len(year_matches_df)} matches")
            year_frames.append(year_matches_df)
            if year < end_year:
                time.sleep(REQUEST_DELAY_SEC)

    if not year_frames:
        raise ValueError("No years were fetched")
    return pd.concat(year_frames, ignore_index=True)


def normalize_types(frame: pd.DataFrame) -> pd.DataFrame:
    # The source incorrectly includes matches of Mbappe's brother (Ethan Mbappe) from PSG U19 in Kylian Mbappe's 2022 statistics.
    frame = frame.loc[frame["competition"] != "UEFA Youth League"].copy()
    frame["match_date"] = pd.to_datetime(frame["match_date"]).dt.date

    integer_columns = [
        "total_time",
        "goals",
        "penalties",
        "missed_penalties",
        "yellow_cards",
        "red_cards",
    ]
    for column in integer_columns:
        frame[column] = pd.to_numeric(frame[column]).astype("Int64")

    return frame[OUTPUT_COLUMNS]
