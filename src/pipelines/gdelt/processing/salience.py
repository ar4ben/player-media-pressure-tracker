import re
import unicodedata
from collections import Counter
from urllib.parse import unquote

import pandas as pd

SALIENCE_OFFSET_THRESHOLD = 100
SALIENCE_WEIGHTS = {
    "lead_or_url_signal": 0.20,
    "position_score": 0.35,
    "density_score": 0.30,
    "rank_score": 0.15,
}


def compute(row: pd.Series) -> pd.Series:
    """
    Compute the salience score and related metrics.

    salience_score =
    0.20 * lead_or_url_signal
    + 0.35 * position_score
    + 0.30 * density_score
    + 0.15 * rank_score
    """

    # V2Persons - Recognized person entities
    person_entries = _parse_offset_entries(row.get("V2Persons"))

    # AllNames - Broader proper names found in the article: people, events, movements, products, etc.
    # Used as fallback for V2Persons
    all_name_entries = _parse_offset_entries(row.get("AllNames"))
    organization_entries = _parse_offset_entries(row.get("V2Organizations"))
    theme_entries = _parse_offset_entries(row.get("V2Themes"))

    all_offset_entries = (
        person_entries + all_name_entries + organization_entries + theme_entries
    )

    player_offset_entries = [
        (name, offset)
        for name, offset in person_entries + all_name_entries
        if _is_player_name(name)
    ]

    first_player_offset = (
        min(offset for _, offset in player_offset_entries)
        if player_offset_entries
        else None
    )

    max_entity_offset = (
        max(offset for _, offset in all_offset_entries) if all_offset_entries else None
    )

    person_counts = Counter(
        _normalize_target_entity(name)
        for name, _ in person_entries
        if _normalize_target_entity(name)
    )

    all_name_counts = Counter(
        _normalize_target_entity(name)
        for name, _ in all_name_entries
        if _normalize_target_entity(name)
    )

    player_mentions, max_mentions_of_any_person = _compute_player_rank_counts(
        person_counts=person_counts,
        all_name_counts=all_name_counts,
    )

    position_score = _compute_position_score(
        first_player_offset=first_player_offset,
        max_entity_offset=max_entity_offset,
    )

    density_score = min(player_mentions, 4) / 4

    rank_score = (
        player_mentions / max_mentions_of_any_person
        if max_mentions_of_any_person > 0
        else 0.0
    )

    url_signal = _url_player_signal(row.get("DocumentIdentifier"))

    lead_signal = (
        1.0
        if first_player_offset is not None
        and first_player_offset <= SALIENCE_OFFSET_THRESHOLD
        else 0.0
    )

    lead_or_url_signal = 1.0 if url_signal == 1.0 or lead_signal == 1.0 else 0.0

    salience_score = (
        SALIENCE_WEIGHTS["lead_or_url_signal"] * lead_or_url_signal
        + SALIENCE_WEIGHTS["position_score"] * position_score
        + SALIENCE_WEIGHTS["density_score"] * density_score
        + SALIENCE_WEIGHTS["rank_score"] * rank_score
    )

    return pd.Series(
        {
            "url_player_signal": url_signal,
            "lead_player_signal": lead_signal,
            "lead_or_url_signal": lead_or_url_signal,
            "first_player_offset": first_player_offset,
            "max_entity_offset": max_entity_offset,
            "position_score": position_score,
            "player_mentions": int(player_mentions),
            "max_mentions_of_any_person": int(max_mentions_of_any_person),
            "density_score": density_score,
            "rank_score": rank_score,
            "salience_score": salience_score,
            "salience_class": _salience_class(salience_score),
        }
    )


def _compute_position_score(
    first_player_offset: int | None, max_entity_offset: int | None
) -> float:
    position_score = 0.0

    if (
        first_player_offset is not None
        and max_entity_offset is not None
        and max_entity_offset > 0
    ):
        position_score = 1.0 - (first_player_offset / max_entity_offset)

    return position_score


def _compute_player_rank_counts(
    person_counts: Counter[str],
    all_name_counts: Counter[str],
) -> tuple[int, int]:
    if person_counts.get("kylian mbappe", 0):
        return person_counts["kylian mbappe"], max(person_counts.values(), default=0)

    return all_name_counts.get("kylian mbappe", 0), max(
        all_name_counts.values(), default=0
    )


def _parse_offset_entries(value) -> list[tuple[str, int]]:
    """
    Parse entries and their offsets in an article:
    'Kylian Mbappe,7323; Nico Paz,5264' -> [('kylian mbappe', 7323), ('nico paz', 5264)]
    Offset is the character position of the entity/name mention found in the article.
    """
    entries: list[tuple[str, int]] = []

    if value is not None and not pd.isna(value):
        for raw_entry in str(value).split(";"):
            entry = raw_entry.strip()
            if "," not in entry:
                continue

            name, offset = entry.rsplit(",", 1)
            name = name.lower().strip()
            offset = offset.strip()

            if offset.isdigit() and name:
                entries.append((name, int(offset)))

    return entries


def _is_player_name(name: str) -> bool:
    return "mbapp" in name


def _normalize_text(value) -> str:
    """
    Apply basic text normalization.
    Examples:
    "José Mourinho"       -> "jose mourinho"
    "Atlético Madrid"     -> "atletico madrid"
    "Bayern München"      -> "bayern munchen"
    "Real Madrid C.F."    -> "real madrid c f"
    "  Nico   Paz!!! "    -> "nico paz"
    """

    if value is None or pd.isna(value):
        return ""

    text = unicodedata.normalize(
        "NFKD", str(value)
    )  # "José-Mourinho" -> ["J", "o", "s", "e", "́", "-", "M", "o", "u", "r", "i", "n", "h", "o"]
    text = "".join(
        ch for ch in text if not unicodedata.combining(ch)
    )  # "José-Mourinho" -> "Jose-Mourinho"
    text = text.lower()  # "Jose-Mourinho" -> "jose-mourinho"
    text = re.sub(r"[^a-z0-9]+", " ", text)  # "jose-mourinho" -> "jose mourinho"
    return re.sub(r"\s+", " ", text).strip()  # " jose   mourinho " -> "jose mourinho"


def _normalize_target_entity(name: str) -> str:
    """
    We normalize known Mbappe variants to one canonical value::
    "Mbappé"        -> "kylian mbappe"
    "Mbappe"        -> "kylian mbappe"
    "Mbapp"         -> "kylian mbappe"
    "Kylian Mbapp"  -> "kylian mbappe"
    "Kylian Mbappé" -> "kylian mbappe"

    Other names are only normalized with basic text cleanup.
    This is not full entity resolution.
    """

    if "mbapp" in name:
        return "kylian mbappe"

    return _normalize_text(name)


def _url_player_signal(document_identifier) -> float:
    text = unquote(str(document_identifier)).lower()
    return 1.0 if "mbapp" in text else 0.0


def _salience_class(score: float) -> str:
    if score >= 0.70:
        return "high_salience"
    if score >= 0.40:
        return "medium_salience"
    return "low_salience"
