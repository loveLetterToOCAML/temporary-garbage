from datetime import datetime, timezone


def utc_now():
    return datetime.now(tz=timezone.utc)

def utc_now_iso():
    return datetime.isoformat(utc_now())

def printable_now(with_micros: bool = False, utc: bool = True):
    if not utc:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') if with_micros else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f') if with_micros else datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

def default_parse_date(date_str: str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
    except:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

def parse_iso_date(date_str: str):
    return datetime.fromisoformat(date_str)


if __name__ == '__main__':
    print(printable_now())
    print(default_parse_date(printable_now()))
    print(printable_now(with_micros=True))
    print(default_parse_date(printable_now(with_micros=True)))
    print(utc_now_iso())
    print(parse_iso_date(utc_now_iso()))
