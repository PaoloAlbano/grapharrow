"""Construct, select and operate on Cypher temporal values."""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from grapharrow.core.errors import QueryError
from grapharrow.core.temporal import (
    AVERAGE_MONTH,
    DAY,
    EPOCH,
    NANOS,
    CalendarDate,
    Duration,
    Instant,
    InstantKind,
    calendar_day,
    trunc_div,
    week_day,
)

INSTANTS = frozenset({"date", "localtime", "time", "localdatetime", "datetime"})
ZONED = frozenset({"time", "datetime"})
CLOCKS = frozenset({"time", "localtime"})
DATE_FIELDS = frozenset(
    {"year", "month", "day", "week", "dayofweek", "ordinalday", "quarter", "dayofquarter"}
)
TIME_FIELDS = frozenset({"hour", "minute", "second", "millisecond", "microsecond", "nanosecond"})
DURATION_FUNCTIONS = frozenset({"between", "inmonths", "indays", "inseconds"})
TEMPORAL_ARITIES = {
    **{name: (0, 1) for name in INSTANTS},
    **{f"{name}.truncate": (1, 2, 3) for name in INSTANTS},
    **{
        f"{name}.{clock}": (0, 1)
        for name in INSTANTS
        for clock in ("transaction", "statement", "realtime")
    },
    **{f"duration.{name}": (2,) for name in DURATION_FUNCTIONS},
    "duration": (1,),
    "datetime.fromepoch": (2,),
    "datetime.fromepochmillis": (1,),
    "timestamp": (0,),
}
_OFFSET = re.compile(r"([+-])([0-9]{2})(?::?([0-9]{2}))?(?::?([0-9]{2}))?")
_TIME = re.compile(
    r"(?P<hour>[0-9]{2})(?:(?P<sep>:?)(?P<minute>[0-9]{2})(?:(?P=sep)(?P<second>[0-9]{2})"
    r"(?:[.,](?P<fraction>[0-9]{1,9}))?)?)?(?P<zone>Z|[+-][0-9:]+)?",
    re.IGNORECASE,
)
_NUMBER = r"[+-]?[0-9]+(?:[.,][0-9]+)?"
_DURATION = re.compile(
    rf"(?P<sign>[+-])?P(?:(?P<years>{_NUMBER})Y)?(?:(?P<months>{_NUMBER})M)?"
    rf"(?:(?P<weeks>{_NUMBER})W)?(?:(?P<days>{_NUMBER})D)?"
    rf"(?P<time>T(?:(?P<hours>{_NUMBER})H)?(?:(?P<minutes>{_NUMBER})M)?"
    rf"(?:(?P<seconds>{_NUMBER})S)?)?",
    re.IGNORECASE,
)
_DURATION_UNITS = {
    "hours": 3600 * NANOS,
    "minutes": 60 * NANOS,
    "seconds": NANOS,
    "milliseconds": 1_000_000,
    "microseconds": 1000,
    "nanoseconds": 1,
}


def _number(value: object) -> Decimal:
    if type(value) not in {int, float, Decimal}:
        raise TypeError("Expected a numeric field")
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("Temporal fields must be finite")
    return result


def _integer(value: object) -> int:
    if type(value) is not int:
        raise TypeError("Expected an integer field")
    assert isinstance(value, int)
    return value


def _fields(value: dict[str, object], allowed: frozenset[str]) -> dict[str, object]:
    fields = {}
    for key, item in value.items():
        if not isinstance(key, str) or key.lower() not in allowed:
            raise ValueError(f"Unknown temporal field: {key}")
        if key.lower() in fields:
            raise ValueError(f"Duplicate temporal field: {key}")
        fields[key.lower()] = item
    return fields


def _duration(fields: dict[str, object]) -> Duration:
    allowed = frozenset({"years", "months", "weeks", "days", *_DURATION_UNITS})
    values = {name: _number(value) for name, value in _fields(fields, allowed).items()}
    zero = Decimal(0)
    months = values.get("years", zero) * 12 + values.get("months", zero)
    days = values.get("weeks", zero) * 7 + values.get("days", zero)
    day_nanos = days * DAY * NANOS + (months - int(months)) * AVERAGE_MONTH * NANOS
    whole_days = int(day_nanos / (DAY * NANOS))
    nanos = sum(values.get(key, zero) * unit for key, unit in _DURATION_UNITS.items())
    return Duration(int(months), whole_days, int(nanos + day_nanos - whole_days * DAY * NANOS))


def _parse_duration(value: str) -> Duration:
    match = _DURATION.fullmatch(value)
    if match:
        fields = {
            key: Decimal(item.replace(",", "."))
            for key, item in match.groupdict().items()
            if item is not None and key not in {"sign", "time"}
        }
        if fields and (not match["time"] or any(key in fields for key in _DURATION_UNITS)):
            if match["sign"] == "-":
                fields = {key: -item for key, item in fields.items()}
            return _duration(dict(fields))
    alternative = re.fullmatch(
        r"([+-])?P([0-9]{4})-?([0-9]{2})-?([0-9]{2})(?:T(.+))?", value, re.IGNORECASE
    )
    if alternative:
        sign, years, months, days, clock = alternative.groups()
        factor = -1 if sign == "-" else 1
        return Duration(
            factor * (int(years) * 12 + int(months)),
            factor * int(days),
            factor * (_parse_clock(clock, zoned=False)[0] if clock else 0),
        )
    raise ValueError("Invalid ISO duration")


def _ordinal(year: int, ordinal: int) -> int:
    result = calendar_day(year) + ordinal - 1
    if ordinal < 1 or result >= calendar_day(year + 1):
        raise ValueError("Ordinal day is outside the year")
    return result


def _parse_date(value: str) -> int:
    if match := re.fullmatch(r"([+-][0-9]{4,9})(?:-([0-9]{2})(?:-([0-9]{2}))?)?", value):
        return calendar_day(int(match[1]), int(match[2] or "1"), int(match[3] or "1"))
    if match := re.fullmatch(r"([0-9]{4})-?W([0-9]{2})(?:-?([1-7]))?", value, re.IGNORECASE):
        return week_day(int(match[1]), int(match[2]), int(match[3] or "1"))
    if match := re.fullmatch(r"([0-9]{4})-?([0-9]{3})", value):
        return _ordinal(int(match[1]), int(match[2]))
    if match := re.fullmatch(r"([0-9]{4})(?:([\-]?)([0-9]{2})(?:\2([0-9]{2}))?)?", value):
        return calendar_day(int(match[1]), int(match[3] or "1"), int(match[4] or "1"))
    raise ValueError("Invalid ISO date")


def _parse_offset(value: str) -> int:
    if value.upper() == "Z":
        return 0
    match = _OFFSET.fullmatch(value)
    if match is None:
        raise ValueError("Invalid timezone offset")
    hours, minutes, seconds = (int(part or "0") for part in match.groups()[1:])
    offset = hours * 3600 + minutes * 60 + seconds
    if minutes >= 60 or seconds >= 60 or offset > 18 * 3600:
        raise ValueError("Invalid timezone offset")
    return -offset if match[1] == "-" else offset


def _clock(hour: int, minute: int, second: int, nano: int) -> int:
    if not (0 <= hour < 24 and 0 <= minute < 60 and 0 <= second < 60 and 0 <= nano < NANOS):
        raise ValueError("Invalid time component")
    return (hour * 3600 + minute * 60 + second) * NANOS + nano


def _parse_clock(value: str, *, zoned: bool) -> tuple[int, int | None]:
    match = _TIME.fullmatch(value)
    if match is None or (not zoned and match["zone"]):
        raise ValueError("Invalid ISO time")
    clock = _clock(
        int(match["hour"]),
        int(match["minute"] or "0"),
        int(match["second"] or "0"),
        int((match["fraction"] or "").ljust(9, "0")),
    )
    return clock, _parse_offset(match["zone"]) if match["zone"] else None


def _timezone(value: object) -> tuple[tzinfo, str]:
    if not isinstance(value, str) or not value:
        raise TypeError("timezone must be a string")
    if value.upper() == "Z" or value.startswith(("+", "-")):
        return timezone(timedelta(seconds=_parse_offset(value))), ""
    # Match the canonical IDs used by Cypher for renamed IANA zones.
    if value == "US/Pacific":
        value = "America/Los_Angeles"
    return ZoneInfo(value), value


def _seconds(value: timedelta | None) -> int:
    assert value is not None
    return value.days * DAY + value.seconds


def _epoch_datetime(seconds: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)


def _from_epoch(
    kind: InstantKind, epoch_nanoseconds: int, tz: tzinfo = UTC, zone: str = ""
) -> Instant:
    if isinstance(tz, timezone):
        offset = _seconds(tz.utcoffset(None))
        day, clock = divmod(epoch_nanoseconds + offset * NANOS, DAY * NANOS)
        return Instant(
            kind,
            0 if kind in CLOCKS else day,
            0 if kind == "date" else clock,
            offset if kind in ZONED else 0,
        )
    seconds, fraction = divmod(epoch_nanoseconds, NANOS)
    current = _epoch_datetime(seconds).astimezone(tz)
    day = (current.date() - EPOCH).days
    clock = _clock(current.hour, current.minute, current.second, fraction)
    offset = _seconds(current.utcoffset()) if kind in ZONED else 0
    return Instant(
        kind,
        0 if kind in CLOCKS else day,
        0 if kind == "date" else clock,
        offset,
        zone if kind == "datetime" else "",
    )


def _with_zone(
    kind: InstantKind,
    day: int,
    clock: int,
    tz: tzinfo,
    zone: str,
    *,
    preferred_offset: int | None = None,
    strict: bool = False,
) -> Instant:
    if isinstance(tz, timezone):
        offset = _seconds(tz.utcoffset(None))
        if strict and offset != preferred_offset:
            raise ValueError("Timezone and offset do not agree")
        return Instant(kind, day, clock, offset)
    naive = _epoch_datetime(day * DAY + clock // NANOS).replace(tzinfo=None)
    current = naive.replace(tzinfo=tz)
    if preferred_offset is not None and _seconds(current.utcoffset()) != preferred_offset:
        second = current.replace(fold=1)
        if _seconds(second.utcoffset()) == preferred_offset:
            current = second
        elif strict:
            raise ValueError("Timezone and offset do not agree")
    epoch = (day * DAY - _seconds(current.utcoffset())) * NANOS + clock
    result = _from_epoch(kind, epoch, tz, zone)
    if strict and (result.clock != clock or result.offset != preferred_offset):
        raise ValueError("Offset is invalid for this wall-clock time")
    return result


def _parse_instant(kind: InstantKind, value: str) -> Instant:
    if kind == "date":
        return Instant(kind, _parse_date(value))
    day, raw_clock = 0, value
    if "datetime" in kind:
        raw_date, _, raw_clock = (
            re.split(r"(T)", value, maxsplit=1, flags=re.IGNORECASE)
            if ("T" in value.upper())
            else (value, "", "00")
        )
        day = _parse_date(raw_date)
    zone_id = ""
    if kind == "datetime" and raw_clock.endswith("]") and "[" in raw_clock:
        raw_clock, zone_id = raw_clock[:-1].rsplit("[", 1)
    clock, offset = _parse_clock(raw_clock, zoned=kind in ZONED)
    if zone_id:
        tz, zone = _timezone(zone_id)
        return _with_zone(
            kind, day, clock, tz, zone, preferred_offset=offset, strict=offset is not None
        )
    return Instant(kind, day, clock, offset or 0)


def _date_from_fields(fields: dict[str, object], base: Instant | None) -> int:
    def pick(key: str, fallback: int) -> int:
        return _integer(fields[key]) if key in fields else fallback

    current = base.calendar if base else CalendarDate(1970, 1, 1)
    if fields.keys() & {"week", "dayofweek"}:
        if fields.keys() & {"month", "day", "ordinalday", "quarter", "dayofquarter"}:
            raise ValueError("Week fields cannot be mixed with calendar fields")
        week_year, week, weekday = current.isocalendar()
        result = week_day(
            pick("year", week_year),
            pick("week", week if base else 1),
            pick("dayofweek", weekday if base else 1),
        )
    elif "ordinalday" in fields:
        if fields.keys() & {"month", "day", "quarter", "dayofquarter"}:
            raise ValueError("Ordinal day cannot be mixed with month or quarter fields")
        result = _ordinal(pick("year", current.year), pick("ordinalday", 1))
    elif fields.keys() & {"quarter", "dayofquarter"}:
        if fields.keys() & {"month", "day"}:
            raise ValueError("Quarter fields cannot be mixed with month fields")
        quarter = (current.month - 1) // 3 + 1
        start = calendar_day(current.year, (quarter - 1) * 3 + 1, 1)
        target = pick("quarter", quarter)
        year = pick("year", current.year)
        first = calendar_day(year, (target - 1) * 3 + 1, 1)
        after_quarter = calendar_day(year, target * 3 + 1) if target < 4 else calendar_day(year + 1)
        ordinal = pick("dayofquarter", base.day - start + 1 if base else 1)
        result = first + ordinal - 1
        if ordinal < 1 or result >= after_quarter:
            raise ValueError("Day is outside the quarter")
    else:
        result = calendar_day(
            pick("year", current.year), pick("month", current.month), pick("day", current.day)
        )
    return result


def _time_from_fields(fields: dict[str, object], base: Instant | None) -> int:
    second, nano = divmod(base.clock if base else 0, NANOS)
    hour, minute = divmod(second // 60, 60)

    def pick(key: str, fallback: int) -> int:
        return _integer(fields[key]) if key in fields else fallback

    if fields.keys() & {"millisecond", "microsecond", "nanosecond"}:
        nano = 0
        scale = NANOS
        for key, unit in (("millisecond", 1_000_000), ("microsecond", 1000), ("nanosecond", 1)):
            if key in fields:
                component = pick(key, 0)
                if not 0 <= component < scale // unit:
                    raise ValueError(f"Invalid {key} field")
                nano += component * unit
                scale = unit
    return _clock(pick("hour", hour), pick("minute", minute), pick("second", second % 60), nano)


def _select_instant(kind: InstantKind, source: object, now: int) -> Instant:
    if isinstance(source, Instant):
        key = "date" if kind == "date" else "time" if kind in CLOCKS else "datetime"
        source = {key: source}
    if not isinstance(source, dict):
        raise TypeError("Temporal input must be a string, map or temporal value")
    allowed = frozenset({"timezone"})
    if kind not in CLOCKS:
        allowed |= DATE_FIELDS | {"date"}
    if kind != "date":
        allowed |= TIME_FIELDS | {"time"}
    if "datetime" in kind:
        allowed |= {"datetime"}
    if kind == "datetime":
        allowed |= {"epochseconds", "epochmillis"}
    fields = _fields(source, allowed)

    def select(key: str, exclude: frozenset[str]) -> Instant | None:
        if key not in fields:
            return None
        selected = fields[key]
        if not isinstance(selected, Instant) or selected.kind in exclude:
            raise TypeError(f"Invalid temporal selector: {key}")
        return selected

    zone_field = fields.get("timezone", "Z")
    tz, zone = _timezone(zone_field)
    if set(fields) <= {"timezone"}:
        return _from_epoch(kind, now, tz, zone)
    if fields.keys() & {"epochseconds", "epochmillis"}:
        key = "epochseconds" if "epochseconds" in fields else "epochmillis"
        if fields.keys() - {key, "timezone"}:
            raise ValueError("Epoch fields cannot be combined with other time or calendar fields")
        return _from_epoch(
            kind, _integer(fields[key]) * (NANOS if key == "epochseconds" else 10**6), tz, zone
        )
    whole = select("datetime", frozenset({"date", *CLOCKS}))
    if whole is not None and fields.keys() & {"date", "time"}:
        raise ValueError("datetime cannot be combined with date or time selectors")
    base_date = whole or select("date", CLOCKS)
    base_time = whole or select("time", frozenset({"date"}))
    day = _date_from_fields(fields, base_date) if kind not in CLOCKS else 0
    clock = _time_from_fields(fields, base_time) if kind != "date" else 0
    if kind not in ZONED:
        return Instant(kind, day, clock)
    if base_time is not None and base_time.kind in ZONED:
        offset = base_time.offset
        if "timezone" in fields:
            base_day = day if kind == "datetime" else base_time.day
            if kind == "datetime" and base_time.zone:
                offset = _with_zone(
                    kind,
                    day,
                    clock,
                    ZoneInfo(base_time.zone),
                    base_time.zone,
                    preferred_offset=offset,
                ).offset
            epoch = (base_day * DAY - offset) * NANOS + clock
            if kind == "time" and base_time.kind == "time" and zone:
                epoch += now // (DAY * NANOS) * DAY * NANOS
            return _from_epoch(kind, epoch, tz, zone)
        if "timezone" not in fields:
            if base_time.zone and kind == "datetime":
                return _with_zone(
                    kind,
                    day,
                    clock,
                    ZoneInfo(base_time.zone),
                    base_time.zone,
                    preferred_offset=offset,
                )
            return Instant(kind, day, clock, offset)
    if kind == "time" and zone:
        offset = _seconds(_epoch_datetime(now // NANOS).astimezone(tz).utcoffset())
        return Instant(kind, clock=clock, offset=offset)
    return _with_zone(kind, day, clock, tz, zone)


def evaluate_temporal(
    name: str, arguments: list[object], now: int
) -> Instant | Duration | int | None:
    if name.endswith(".truncate") and len(arguments) == 3 and arguments[2] is None:
        arguments = arguments[:2]
    if any(arg is None for arg in arguments):
        return None
    function, _, operation = name.partition(".")
    try:
        if name == "timestamp":
            return now // 1_000_000
        if function == "duration":
            if operation:
                start, end = arguments
                if not isinstance(start, Instant) or not isinstance(end, Instant):
                    raise TypeError("Duration differences require two temporal instants")
                return _difference(operation, start, end)
            source = arguments[0]
            if isinstance(source, Duration):
                return source
            if isinstance(source, str):
                return _parse_duration(source)
            if isinstance(source, dict):
                return _duration(source)
            raise TypeError("Duration input must be a string or map")
        kind = cast(InstantKind, function)
        if operation == "fromepoch":
            return _from_epoch(kind, _integer(arguments[0]) * NANOS + _integer(arguments[1]))
        if operation == "fromepochmillis":
            return _from_epoch(kind, _integer(arguments[0]) * 1_000_000)
        if operation == "truncate":
            return _truncate(kind, arguments, now)
        if operation:
            source = {"timezone": arguments[0]} if arguments else {}
        else:
            source = arguments[0] if arguments else {}
        if isinstance(source, str):
            return _parse_instant(kind, source)
        return _select_instant(kind, source, now)
    except TypeError as error:
        raise QueryError.invalid_type(f"{name}(): {error}", phase="runtime") from error
    except (ValueError, ArithmeticError, ZoneInfoNotFoundError) as error:
        raise QueryError(
            f"{name}(): {error}",
            category="ArgumentError",
            phase="runtime",
            detail="InvalidArgumentValue",
        ) from error


def _with_fields(value: Instant, fields: dict[str, object], now: int) -> Instant:
    key = "date" if value.kind == "date" else "time" if value.kind in CLOCKS else "datetime"
    return _select_instant(value.kind, {key: value, **fields}, now)


def _truncate(kind: InstantKind, arguments: list[object], now: int) -> Instant:
    unit = arguments[0]
    if not isinstance(unit, str):
        raise TypeError("Truncation unit must be a string")
    source = arguments[1] if len(arguments) > 1 else _from_epoch(kind, now)
    fields = arguments[2] if len(arguments) > 2 else {}
    if not isinstance(source, Instant) or not isinstance(fields, dict):
        raise TypeError("Expected a temporal instant and a map")
    day, clock = source.day, source.clock
    original = source.calendar
    date_units = {"millennium": 1000, "century": 100, "decade": 10, "year": 1}
    clock_units = {
        "day": DAY * NANOS,
        "hour": 3600 * NANOS,
        "minute": 60 * NANOS,
        "second": NANOS,
        "millisecond": 1_000_000,
        "microsecond": 1000,
        "nanosecond": 1,
    }
    if unit in date_units:
        scale = date_units[unit]
        day = calendar_day(original.year // scale * scale)
        clock = 0
    elif unit == "weekYear":
        day = week_day(original.isocalendar()[0])
        clock = 0
    elif unit in {"quarter", "month", "week"}:
        if unit == "week":
            day -= original.isocalendar()[2] - 1
        else:
            month = (original.month - 1) // 3 * 3 + 1 if unit == "quarter" else original.month
            day = calendar_day(original.year, month)
        clock = 0
    elif unit in clock_units and (kind != "date" or unit == "day"):
        clock = clock // clock_units[unit] * clock_units[unit]
    else:
        raise ValueError(f"Invalid truncation unit: {unit}")
    if (kind in CLOCKS and unit not in clock_units) or (
        source.kind in CLOCKS and kind not in CLOCKS
    ):
        raise ValueError(f"Invalid truncation unit or input for {kind}")
    if kind in CLOCKS:
        day = 0
    if kind in ZONED and source.zone:
        result = _with_zone(
            kind, day, clock, ZoneInfo(source.zone), source.zone, preferred_offset=source.offset
        )
    else:
        result = Instant(kind, day, clock, source.offset if kind in ZONED else 0)
    if not fields:
        return result
    updates = _fields(fields, DATE_FIELDS | TIME_FIELDS | {"timezone"})
    if unit in {"millisecond", "microsecond"}:
        updates.setdefault("millisecond", clock % NANOS // 1_000_000)
        if unit == "microsecond":
            updates.setdefault("microsecond", clock % 1_000_000 // 1000)
    if "timezone" in updates and kind in ZONED:
        # Replace the zone of the truncated wall clock.
        if kind == "time":
            return _select_instant(
                kind, {"time": Instant("localtime", clock=clock), **updates}, now
            )
        return _select_instant(
            kind, {"datetime": Instant("localdatetime", day, clock), **updates}, now
        )
    return _with_fields(result, updates, now)


def _shift(instant: Instant, duration: Duration) -> Instant:
    if instant.kind in CLOCKS:
        return replace(instant, clock=(instant.clock + duration.nanos) % (DAY * NANOS))
    day = instant.calendar.add_months(duration.months) + duration.days
    if instant.kind == "date":
        return replace(instant, day=day + trunc_div(duration.nanos, DAY * NANOS))
    if instant.zone:
        tz = ZoneInfo(instant.zone)
        local = _with_zone(
            instant.kind, day, instant.clock, tz, instant.zone, preferred_offset=instant.offset
        )
        return _from_epoch(
            instant.kind,
            local.epoch_seconds * NANOS + local.clock % NANOS + duration.nanos,
            tz,
            instant.zone,
        )
    days, clock = divmod(instant.clock + duration.nanos, DAY * NANOS)
    return replace(instant, day=day + days, clock=clock)


def _difference(operation: str, start: Instant, end: Instant) -> Duration:
    both_dates = start.kind not in CLOCKS and end.kind not in CLOCKS
    months, days = 0, 0
    end_clock = end.clock
    if both_dates and start.kind == end.kind == "datetime":
        end_clock += (start.offset - end.offset) * NANOS
    end_shift, end_clock = divmod(end_clock, DAY * NANOS)
    comparison_day = end.day + end_shift
    if both_dates and operation in {"between", "inmonths"}:
        a, b = start.calendar, CalendarDate.from_epoch_day(comparison_day)
        # Compare field positions before clamping to the end of a shorter month.
        months = (b.year - a.year) * 12 + b.month - a.month
        if months > 0 and (b.day, end_clock) < (a.day, start.clock):
            months -= 1
        elif months < 0 and (b.day, end_clock) > (a.day, start.clock):
            months += 1
        if operation == "inmonths":
            return Duration(months)
        start = _shift(start, Duration(months))
    if both_dates and operation in {"between", "indays"}:
        days = comparison_day - start.day
        if days > 0 and end_clock < start.clock:
            days -= 1
        elif days < 0 and end_clock > start.clock:
            days += 1
        if operation == "indays":
            return Duration(days=days)
        start = _shift(start, Duration(days=days))
    if operation in {"indays", "inmonths"}:
        return Duration()
    delta = end.clock - start.clock
    if both_dates:
        delta += (end.day - start.day) * DAY * NANOS
    if start.kind in ZONED and end.kind in ZONED:
        delta += (start.offset - end.offset) * NANOS
    elif start.zone or end.zone:
        zoned, local = (start, end) if start.zone else (end, start)
        day = local.day if local.kind not in CLOCKS else zoned.day
        selected = _with_zone("datetime", day, local.clock, ZoneInfo(zoned.zone), zoned.zone)
        adjust = zoned.offset - selected.offset
        delta += (adjust if start.zone else -adjust) * NANOS
    return Duration(months, days, delta)


def temporal_arithmetic(operator: str, left: object, right: object) -> Instant | Duration:
    try:
        if operator in {"+", "-"} and isinstance(right, Duration):
            duration = (
                right if operator == "+" else Duration(-right.months, -right.days, -right.nanos)
            )
            if isinstance(left, Instant):
                return _shift(left, duration)
            if isinstance(left, Duration):
                return Duration(
                    left.months + duration.months,
                    left.days + duration.days,
                    left.nanos + duration.nanos,
                )
        if operator == "+" and isinstance(left, Duration) and isinstance(right, Instant):
            return _shift(right, left)
        if operator == "*" and isinstance(right, Duration):
            left, right = right, left
        if operator in {"*", "/"} and isinstance(left, Duration):
            factor = _number(right)
            if operator == "/":
                if factor == 0:
                    raise ZeroDivisionError("Division by zero")
                factor = Decimal(1) / factor
            return _duration(
                {
                    "months": left.months * factor,
                    "days": left.days * factor,
                    "nanoseconds": left.nanos * factor,
                }
            )
    except ZeroDivisionError as error:
        raise QueryError(
            "Division by zero", category="ArithmeticError", phase="runtime", detail="DivisionByZero"
        ) from error
    except (TypeError, ValueError, ArithmeticError) as error:
        raise QueryError(
            str(error),
            category="ArithmeticError",
            phase="runtime",
            detail="InvalidArithmeticOperation",
        ) from error
    raise QueryError.invalid_type(f"Invalid temporal operands for {operator}", phase="runtime")


def datetime_from_wire(seconds: int, nanos: int, zone: str | int | None, *, utc: bool) -> Instant:
    """Reconstruct local, legacy or UTC-patched Bolt date times."""
    if not 0 <= nanos < NANOS:
        raise ValueError("Invalid nanoseconds")
    day, second = divmod(seconds, DAY)
    clock = second * NANOS + nanos
    if zone is None:
        return Instant("localdatetime", day, clock)
    if isinstance(zone, int):
        if abs(zone) > 18 * 3600:
            raise ValueError("Invalid zone offset")
        if utc:
            day, clock = divmod((seconds + zone) * NANOS + nanos, DAY * NANOS)
        return Instant("datetime", day, clock, zone)
    tz, zone_id = _timezone(zone)
    if utc:
        return _from_epoch("datetime", seconds * NANOS + nanos, tz, zone_id)
    return _with_zone("datetime", day, clock, tz, zone_id)
