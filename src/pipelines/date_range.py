from collections.abc import Iterator
from datetime import date, timedelta


def generate_days_from_range(
    start_date: date | str,
    end_date: date | str,
) -> Iterator[date]:
    start = _to_date(start_date)
    end = _to_date(end_date)
    validate_date_range(start, end)

    current_date = start
    while current_date <= end:
        yield current_date
        current_date += timedelta(days=1)


def generate_intervals_from_range(
    start_date: str,
    end_date: str,
    interval_days: int,
) -> list[tuple[str, str]]:
    """Split a date range into inclusive fixed-size day chunks."""

    if interval_days <= 0:
        raise ValueError(f"interval_days must be positive: {interval_days}")

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    validate_date_range(start, end)

    intervals: list[tuple[str, str]] = []
    current_start = start

    while current_start <= end:
        current_end = min(end, current_start + timedelta(days=interval_days - 1))

        intervals.append((current_start.isoformat(), current_end.isoformat()))
        current_start = current_end + timedelta(days=1)

    return intervals


def validate_date_range(start_date: date, end_date: date) -> None:
    if start_date > end_date:
        msg = f"Start date must be <= End date: {start_date=} {end_date=}"
        raise ValueError(msg)


def _to_date(value: date | str) -> date:
    if isinstance(value, str):
        return date.fromisoformat(value)
    return value
