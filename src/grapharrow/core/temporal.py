"""Nanosecond-precision Cypher temporal values and their ISO representations."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from typing import Literal

NANOS = 1_000_000_000
DAY = 86_400
EPOCH = date(1970, 1, 1)
AVERAGE_MONTH = 2_629_746
_YEARS_PER_CYCLE = 400
_DAYS_PER_CYCLE = 146_097
_MIN_YEAR = -999_999_999
_MAX_YEAR = 999_999_999
InstantKind = Literal["date", "localtime", "time", "localdatetime", "datetime"]


def trunc_div(value: int, unit: int) -> int:
    return -(abs(value) // unit) if value < 0 else value // unit


def offset_text(offset: int) -> str:
    if not offset:
        return "Z"
    minute, second = divmod(abs(offset), 60)
    hour, minute = divmod(minute, 60)
    return f"{'-' if offset < 0 else '+'}{hour:02d}:{minute:02d}" + (
        f":{second:02d}" if second else ""
    )


def calendar_day(year: int, month: int = 1, day: int = 1) -> int:
    """Return the epoch day, using the repeat cycle of the Gregorian calendar."""
    cycles, base_year = divmod(year - 1, _YEARS_PER_CYCLE)
    ordinal = date(base_year + 1, month, day).toordinal() + cycles * _DAYS_PER_CYCLE
    return ordinal - EPOCH.toordinal()


def week_day(year: int, week: int = 1, weekday: int = 1) -> int:
    # Use a cycle away from the ends of the stdlib's range and ISO week spillover.
    cycles, base_year = divmod(year - 2000, _YEARS_PER_CYCLE)
    used = date.fromisocalendar(base_year + 2000, week, weekday)
    return (used - EPOCH).days + cycles * _DAYS_PER_CYCLE


@dataclass(frozen=True, slots=True)
class CalendarDate:
    year: int
    month: int
    day: int

    @classmethod
    def from_epoch_day(cls, day: int) -> CalendarDate:
        cycles, ordinal = divmod(day + EPOCH.toordinal() - 1, _DAYS_PER_CYCLE)
        base = date.fromordinal(ordinal + 1)
        return cls(base.year + cycles * _YEARS_PER_CYCLE, base.month, base.day)

    def isoformat(self) -> str:
        year = (
            f"{self.year:04d}"
            if 0 <= self.year <= 9999
            else (f"{'-' if self.year < 0 else '+'}{abs(self.year):04d}")
        )
        return f"{year}-{self.month:02d}-{self.day:02d}"

    def isocalendar(self) -> tuple[int, int, int]:
        cycles, base_year = divmod(self.year - 2000, _YEARS_PER_CYCLE)
        week_year, week, weekday = date(base_year + 2000, self.month, self.day).isocalendar()
        return week_year + cycles * _YEARS_PER_CYCLE, week, weekday

    def add_months(self, months: int) -> int:
        year, month = divmod(self.year * 12 + self.month - 1 + months, 12)
        return calendar_day(year, month + 1, min(self.day, monthrange(year, month + 1)[1]))


_MIN_DAY = calendar_day(_MIN_YEAR)
_MAX_DAY = calendar_day(_MAX_YEAR, 12, 31)


@dataclass(frozen=True, slots=True)
class Instant:
    kind: InstantKind
    day: int = 0
    clock: int = 0
    offset: int = 0
    zone: str = ""

    def __post_init__(self) -> None:
        if not _MIN_DAY <= self.day <= _MAX_DAY:
            raise ValueError("Calendar year must be between -999999999 and +999999999")
        if not 0 <= self.clock < DAY * NANOS:
            raise ValueError("Time is outside the day")
        if abs(self.offset) > 18 * 3600:
            raise ValueError("Timezone offset must be between -18:00 and +18:00")

    @property
    def calendar(self) -> CalendarDate:
        return CalendarDate.from_epoch_day(self.day)

    @property
    def epoch_seconds(self) -> int:
        return self.day * DAY + self.clock // NANOS - self.offset

    def order_key(self) -> tuple[int, int, int, str]:
        return self.epoch_seconds, self.clock % NANOS, self.offset, self.zone

    @property
    def type_name(self) -> str:
        return {
            "date": "Date",
            "localtime": "LocalTime",
            "time": "Time",
            "localdatetime": "LocalDateTime",
            "datetime": "DateTime",
        }[self.kind]

    def __str__(self) -> str:
        if self.kind == "date":
            return self.calendar.isoformat()
        seconds, fraction = divmod(self.clock, NANOS)
        minute, second = divmod(seconds, 60)
        hour, minute = divmod(minute, 60)
        clock = f"{hour:02d}:{minute:02d}"
        if second or fraction:
            clock += f":{second:02d}"
        if fraction:
            digits = 3 if fraction % 1_000_000 == 0 else 6 if fraction % 1000 == 0 else 9
            clock += f".{fraction:09d}"[: digits + 1]
        if self.kind in {"time", "datetime"}:
            clock += offset_text(self.offset)
        if self.zone:
            clock += f"[{self.zone}]"
        return f"{self.calendar.isoformat()}T{clock}" if "datetime" in self.kind else clock

    def component(self, key: str) -> int | str:
        if self.kind not in {"time", "localtime"}:
            calendar = self.calendar
            week_year, week, week_day = calendar.isocalendar()
            quarter = (calendar.month - 1) // 3
            fields = {
                "year": calendar.year,
                "quarter": quarter + 1,
                "month": calendar.month,
                "week": week,
                "weekYear": week_year,
                "day": calendar.day,
                "ordinalDay": self.day - calendar_day(calendar.year) + 1,
                "weekDay": week_day,
                "dayOfWeek": week_day,
                "dayOfQuarter": self.day - calendar_day(calendar.year, quarter * 3 + 1) + 1,
            }
            if key in fields:
                return fields[key]
        if self.kind != "date":
            seconds, nanosecond = divmod(self.clock, NANOS)
            fields = {
                "hour": seconds // 3600,
                "minute": seconds // 60 % 60,
                "second": seconds % 60,
                "millisecond": nanosecond // 1_000_000,
                "microsecond": nanosecond // 1000,
                "nanosecond": nanosecond,
            }
            if key in fields:
                return fields[key]
        if self.kind in {"time", "datetime"}:
            if key in {"timezone", "offset"}:
                return self.zone if key == "timezone" and self.zone else offset_text(self.offset)
            if key == "offsetSeconds":
                return self.offset
            if key == "offsetMinutes":
                return trunc_div(self.offset, 60)
            if self.kind == "datetime":
                if key == "epochSeconds":
                    return self.epoch_seconds
                if key == "epochMillis":
                    return self.epoch_seconds * 1000 + self.clock % NANOS // 1_000_000
        raise KeyError(key)


@dataclass(frozen=True, slots=True)
class Duration:
    months: int = 0
    days: int = 0
    nanos: int = 0

    def __post_init__(self) -> None:
        if any(
            not -(2**63) <= value < 2**63 for value in (self.months, self.days, self.nanos // NANOS)
        ):
            raise OverflowError("Duration component exceeds the signed 64-bit range")

    def order_key(self) -> tuple[int, int, int, int]:
        total = (self.months * AVERAGE_MONTH + self.days * DAY) * NANOS + self.nanos
        return total, self.months, self.days, self.nanos

    def __str__(self) -> str:
        years = trunc_div(self.months, 12)
        months = self.months - years * 12
        hours = trunc_div(self.nanos, 3600 * NANOS)
        remainder = self.nanos - hours * 3600 * NANOS
        minutes = trunc_div(remainder, 60 * NANOS)
        remainder -= minutes * 60 * NANOS
        seconds, nanos = divmod(abs(remainder), NANOS)
        time = (f"{hours}H" if hours else "") + (f"{minutes}M" if minutes else "")
        if remainder:
            decimal = f".{nanos:09d}".rstrip("0") if nanos else ""
            time += f"{'-' if remainder < 0 else ''}{seconds}{decimal}S"
        calendar = (f"{years}Y" if years else "") + (f"{months}M" if months else "")
        calendar += f"{self.days}D" if self.days else ""
        return "P" + calendar + (f"T{time}" if time else "" if calendar else "T0S")

    def component(self, key: str) -> int:
        units = {
            "years": trunc_div(self.months, 12),
            "quarters": trunc_div(self.months, 3),
            "months": self.months,
            "weeks": trunc_div(self.days, 7),
            "days": self.days,
            "hours": trunc_div(self.nanos, 3600 * NANOS),
            "minutes": trunc_div(self.nanos, 60 * NANOS),
            "seconds": self.nanos // NANOS,
            "milliseconds": self.nanos // 1_000_000,
            "microseconds": self.nanos // 1000,
            "nanoseconds": self.nanos,
        }
        if key in units:
            return units[key]
        unit, _, parent = key.partition("Of")
        if parent == "Second" and unit in {"milliseconds", "microseconds", "nanoseconds"}:
            return (
                self.nanos
                % NANOS
                // {"milliseconds": 1_000_000, "microseconds": 1000, "nanoseconds": 1}[unit]
            )
        modulus = {
            ("quarters", "Year"): 4,
            ("months", "Quarter"): 3,
            ("months", "Year"): 12,
            ("days", "Week"): 7,
            ("minutes", "Hour"): 60,
            ("seconds", "Minute"): 60,
        }[(unit, parent)]
        value = units[unit]
        return value - trunc_div(value, modulus) * modulus


class TemporalResult(str):
    """JSON-compatible ISO string retaining its type for Bolt serialization."""

    temporal: Instant | Duration

    def __new__(cls, temporal: Instant | Duration) -> TemporalResult:
        result = super().__new__(cls, str(temporal))
        result.temporal = temporal
        return result

    def __reduce__(self) -> tuple[type[TemporalResult], tuple[Instant | Duration]]:
        return type(self), (self.temporal,)
