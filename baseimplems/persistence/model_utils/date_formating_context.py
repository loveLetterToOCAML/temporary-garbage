from baseimplems.anyio_utils import run_within

from datetime import datetime, timezone
from contextvars import ContextVar
import enum


class DateFormat(enum.Enum):
    FULL_WITH_TIME = enum.auto()
    ONLY_NUMBERS = enum.auto()
    SHORT_TEXT = enum.auto()
    FULL_TEXT_WITH_TIME = enum.auto()


date_format_policy = ContextVar[DateFormat]('date_format', default=DateFormat.FULL_WITH_TIME)
run_with_date_format_policy = run_within(DateFormat, date_format_policy)


def current_date(utc: bool = True):
    now = datetime.now(tz=timezone.utc) if utc else datetime.now()
    dfp = date_format_policy.get()
    if dfp == DateFormat.FULL_WITH_TIME:
        return now.strftime("%d/%m/%Y %H:%M:%S")
    elif dfp == DateFormat.ONLY_NUMBERS:
        return now.strftime("%d/%m/%Y")
    elif dfp == DateFormat.SHORT_TEXT:
        return now.strftime("%b-%d-%Y")
    elif dfp == DateFormat.FULL_TEXT_WITH_TIME:
        return now.strftime("%B %d, %Y - %H:%M:%S")
    else:
        raise NotImplementedError


if __name__ == '__main__':
    import anyio

    async def main():
        async with run_with_date_format_policy(DateFormat.SHORT_TEXT):
            print(current_date())
        async with run_with_date_format_policy(DateFormat.FULL_WITH_TIME):
            print(current_date())

    anyio.run(main)
