import datetime
import dateutil.tz


def get_utc_now():
    """
    Return a time-zone aware DateTime object with the current date and time
    """
    return datetime.datetime.utcnow().replace(tzinfo=dateutil.tz.tzutc())
