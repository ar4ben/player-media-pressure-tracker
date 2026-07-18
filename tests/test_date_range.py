from datetime import date

import pytest

from pipelines import date_range


def test_generate_days_from_range_is_inclusive():
    result = list(date_range.generate_days_from_range("2025-01-01", "2025-01-03"))
    assert result == [
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 1, 3),
    ]


def test_generate_intervals_builds_fixed_size_day_ranges():
    intervals = date_range.generate_intervals_from_range(
        start_date="2024-01-01",
        end_date="2024-12-31",
        interval_days=90,
    )

    assert intervals == [
        ("2024-01-01", "2024-03-30"),
        ("2024-03-31", "2024-06-28"),
        ("2024-06-29", "2024-09-26"),
        ("2024-09-27", "2024-12-25"),
        ("2024-12-26", "2024-12-31"),
    ]


def test_generate_intervals_preserves_partial_boundaries():
    intervals = date_range.generate_intervals_from_range(
        start_date="2024-02-15",
        end_date="2024-09-10",
        interval_days=30,
    )

    assert intervals == [
        ("2024-02-15", "2024-03-15"),
        ("2024-03-16", "2024-04-14"),
        ("2024-04-15", "2024-05-14"),
        ("2024-05-15", "2024-06-13"),
        ("2024-06-14", "2024-07-13"),
        ("2024-07-14", "2024-08-12"),
        ("2024-08-13", "2024-09-10"),
    ]


def test_generate_intervals_can_build_daily_ranges():
    intervals = date_range.generate_intervals_from_range(
        start_date="2024-11-29",
        end_date="2024-12-02",
        interval_days=1,
    )

    assert intervals == [
        ("2024-11-29", "2024-11-29"),
        ("2024-11-30", "2024-11-30"),
        ("2024-12-01", "2024-12-01"),
        ("2024-12-02", "2024-12-02"),
    ]


def test_generate_intervals_rejects_invalid_range():
    with pytest.raises(ValueError, match="Start date must be <= End date"):
        date_range.generate_intervals_from_range(
            start_date="2025-02-01",
            end_date="2025-01-01",
            interval_days=90,
        )


def test_generate_intervals_rejects_non_positive_interval():
    with pytest.raises(ValueError, match="interval_days must be positive"):
        date_range.generate_intervals_from_range(
            start_date="2025-01-01",
            end_date="2025-01-31",
            interval_days=0,
        )
