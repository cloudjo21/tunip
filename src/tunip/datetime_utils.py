import pendulum
import datetime as dt
import math
import numpy as np

from datetime import datetime, time, timedelta, timezone
from itertools import product
from pydantic import BaseModel
from typing import List, Tuple, Union

from tunip.constants import TIME_ZONE


DATETIME_DT_FORMAT = r"%Y-%m-%d %H:%M:%S"
DATETIME_BY_MINUTE_DT_FORMAT = r"%Y-%m-%d %H:%M"
YYMD_FORMAT = r"%Y-%m-%d"

TIME_DT_FORMAT = r"%H:%M:%S"
TIME_BY_MINUTE_DT_FORMAT = r"%H:%M"

SNAPSHOT_DT_FORMAT = r"%Y%m%d_%H%M%S_%f"
SNAPSHOT_DT_FORMAT_WITH_ZEROS = r"%Y%m%d_000000_000000"

DAG_RUNID_FORMAT = r"%Y-%m-%dT%H:%M:%S%z"
DAG_RUNID_MORE_PRECISION_FORMAT = r"%Y-%m-%dT%H:%M:%S.%f%z"
SQL_DATETIME_DT_FORMAT = r"%Y-%m-%dT%H:%M:%S"

TIME_KEYS_PER3HOUR = [str(t[0])+str(t[1]) for t in list(product(list(range(0, 7)), [9, 12, 15, 18, 21, 0]))]

KST = timezone(timedelta(hours=9))


def snapshot2dag_runid(snapshot_dt, time_zone=None):
    tz = time_zone or timezone.utc
    dt = datetime.strptime(snapshot_dt, SNAPSHOT_DT_FORMAT)
    return dt.astimezone(tz).strftime(DAG_RUNID_FORMAT)

def snapshot2date(snapshot_dt: str, time_zone=None) -> str:
    tz = time_zone or timezone.utc
    dt = datetime.strptime(snapshot_dt, SNAPSHOT_DT_FORMAT)
    return str(dt.astimezone(tz).date())

def dag_runid2snapshot(dag_runid, timezone=TIME_ZONE):
    datetime_parts = dag_runid.split('__')[-1]
    try:
        dt = datetime.strptime(datetime_parts, DAG_RUNID_FORMAT)
    except:
        dt = datetime.strptime(datetime_parts, DAG_RUNID_MORE_PRECISION_FORMAT)
    return dt.astimezone(pendulum.timezone(timezone)).strftime(SNAPSHOT_DT_FORMAT)

def to_sql_strftime(dt, timezone=TIME_ZONE) -> str:
    return dt.astimezone(pendulum.timezone(timezone)).strftime(SQL_DATETIME_DT_FORMAT)

def to_snapshot_strftime(dt, timezone=TIME_ZONE) -> str:
    return dt.astimezone(pendulum.timezone(timezone)).strftime(SNAPSHOT_DT_FORMAT)

def yymmdd_to_snapshot_dt_with_zeros(_date: str) -> str:
    return datetime.strptime(_date, YYMD_FORMAT).strftime(SNAPSHOT_DT_FORMAT_WITH_ZEROS)

def yymmdd_time_range_to_datetime(yymd_time_range: str) -> Tuple[dt.datetime, dt.datetime]:
    """_summary_

    Args:
        yymd_time_range (str): yy-mm-dd hh:mm~hh:mm

    Returns:
        Tuple[dt.datetime, dt.datetime]: _description_
    """
    date_and_time_range = yymd_time_range.split()
    head_dt = datetime.strptime(date_and_time_range[0], YYMD_FORMAT)
    hhmm_pair = date_and_time_range[1].split("~")
    start_hour_minute = hhmm_pair[0].split(":")
    end_hour_minute = hhmm_pair[1].split(":")

    start_dt = datetime(head_dt.year, head_dt.month, head_dt.day, int(start_hour_minute[0]), int(start_hour_minute[1]))
    end_dt = datetime(head_dt.year, head_dt.month, head_dt.day, int(end_hour_minute[0]), int(end_hour_minute[1]))

    return start_dt, end_dt


def assign_hour6div(x_time):
    if x_time >= time(9) and x_time < time(12):
        return 9
    elif x_time >= time(12) and x_time < time(15):
        return 12
    elif x_time >= time(15) and x_time < time(18):
        return 15
    elif x_time >= time(18) and x_time < time(21):
        return 18
    else:
        return 21
    # elif x_time >= time(21) or x_time < time(9):
    #     return 21
    # else:
    #     return 0


MIN = 7
def index_to_hour(index):
    index -= 1
    hour = int(math.floor(index / 2)) + MIN
    return hour


def bitmap_to_time_division(bits):
    # bitmap_to_time_division(67094272)
    # bitmap_to_time_division(1071644796)
    if bits == 0:
        return [21]
    range_arr = []
    start_time = ''
    prev_b = False
    for i in range(0, 31):
        b = (bits & (1 << i)) > 0
        if b != prev_b:
            if b:
                start_time = index_to_hour(i + 1)
                range_arr.append(assign_hour6div(time(start_time)))

            prev_b = b

    return range_arr


def _index_to_time(index: int) -> int: 
    # 1로 설정된 범위를 찾고, 범위 시작과 끝을 시간으로 변환
    index -= 1
    hour = int(math.floor(index / 2)) + MIN
    minute = (index % 2) * 30
    time = "%02d%02d" % (hour, minute)
    return int(time)

def bitmap_to_time_range(bits: int) -> list:
    # 30분 단위 비트로 저장된 시간을 시간 범위 list로 변환
    # bitmap_to_time_range(1071644796) --> [{'start': 800, 'end': 1030}, {'start': 1730, 'end': 2200}]
    range_arr = []
    start_time = ""
    prev_b = False
    for i in range(0, 31):
        b = (bits & (1 << i)) > 0
        if b != prev_b:
            if b:
                start_time = _index_to_time(i + 1)
            else:
                end_time = _index_to_time(i + 1)
                time_range = {
                    "start": start_time,
                    "end": end_time
                }
                range_arr.append(time_range)

            prev_b = b

    return range_arr


def get_onehot_vec(time_keys, time_key_divisions):
    onehot_vec = np.zeros(len(time_key_divisions))
    for t in time_keys:
        onehot_vec[time_key_divisions.index(t)] = 1
    return onehot_vec


def concat_for_visit_time_key(x):
    return f"{x.designated_weekday}{x.designated_time_division}"

def concat_for_time_key(x):
    return f"{x.weekday}{x.time_division}"


def dates_between(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += dt.timedelta(days=1)

def get_mondays_sundays_between(start_date, end_date):
    for date in dates_between(start_date, end_date):
        if date.weekday() == 6:  # 6 corresponds to Sunday (0 - Monday, 6 - Sunday)
            date_before_6days = date - dt.timedelta(days=6)
            yield (date_before_6days, date)


def previous_weekday_m_days_before(target_date, target_weekday, m_days=30):
    # First, get the date 'm' days before the target_date
    date_m_days_before = target_date - dt.timedelta(days=m_days)

    # Now, calculate the weekday of date_30_days_before (0 is Monday, 6 is Sunday)
    weekday = date_m_days_before.weekday()

    # If weekday is 0 (Monday) ~ 6 (Sunday), return date_m_days_before
    if weekday == target_weekday:
        return date_m_days_before
    # Otherwise, subtract the appropriate number of days to get the previous Monday
    else:
        return date_m_days_before - dt.timedelta(days=(weekday))


def previous_monday_30_days_before(target_date):
    # First, get the date 30 days before the target_date
    date_30_days_before = target_date - dt.timedelta(days=30)

    # Now, calculate the weekday of date_30_days_before (0 is Monday, 6 is Sunday)
    weekday = date_30_days_before.weekday()

    # If weekday is 0 (Monday), return date_30_days_before
    if weekday == 0:
        return date_30_days_before
    # Otherwise, subtract the appropriate number of days to get the previous Monday
    else:
        return date_30_days_before - dt.timedelta(days=(weekday))


def previous_sunday(target_date):
    # Calculate the weekday of the target_date (0 is Monday, 6 is Sunday)
    weekday = target_date.weekday()

    # If the target_date is Sunday, we want to return the previous Sunday which is 7 days ago
    if weekday == 6:
        return target_date - dt.timedelta(days=7)

    # If the target_date is not Sunday, subtract (weekday+1) to get the previous Sunday
    else:
        return target_date - dt.timedelta(days=(weekday + 1))

def get_datetime_intervals(start_date: str, end_date: str) -> Tuple[List[dt.datetime], List[dt.datetime]]:
    start_date = datetime.strptime(start_date, YYMD_FORMAT)
    end_date = datetime.strptime(end_date, YYMD_FORMAT)

    date_start_intervals = []
    date_end_intervals = []
    for start_day, end_day, in get_mondays_sundays_between(start_date, end_date):
        date_start_intervals.append(start_day)
        date_end_intervals.append(end_day)

    return date_start_intervals, date_end_intervals
  
  
class DateInterval(BaseModel):
    source_start_dates: List[dt.date]
    source_end_dates: List[dt.date]
    target_start_dates: List[dt.date]
    target_end_dates: List[dt.date]

    def __iter__(self):
        return zip(
            self.source_start_dates,
            self.source_end_dates,
            self.target_start_dates,
            self.target_end_dates
        )

    def maximal_interval(self) -> Tuple[dt.date, dt.date]:
        return self.source_start_dates[0], self.target_end_dates[-1]
    
    def target_intervals(self) -> Tuple[List[dt.date], List[dt.date]]:
        return (self.target_start_dates, self.target_end_dates)

class DatetimeInterval(BaseModel):
    source_start_dates: List[dt.datetime]
    source_end_dates: List[dt.datetime]
    target_start_dates: List[dt.datetime]
    target_end_dates: List[dt.datetime]

    def __iter__(self):
        return zip(
            self.source_start_dates,
            self.source_end_dates,
            self.target_start_dates,
            self.target_end_dates
        )

    def maximal_interval(self) -> Tuple[dt.datetime, dt.datetime]:
        return self.source_start_dates[0], self.target_end_dates[-1]


class DatetimeIntervalItem(BaseModel):
    source_start_date: dt.datetime
    source_end_date: dt.datetime
    target_start_date: dt.datetime
    target_end_date: dt.datetime


class DateIntervalBuilder:
    def __init__(self, interval_start_date: str, interval_end_date: str, date_period_days: int=7, date_duration_days: int=None, use_datetime: bool=False):
        self.interval_start_date = interval_start_date
        self.interval_end_date = interval_end_date
        self.date_period_days = date_period_days
        self.date_duration_days = date_duration_days if date_duration_days else self.date_period_days
        self.use_datetime = use_datetime

    def apply(self) -> Union[DateInterval, DatetimeInterval]:
        start_dates, end_dates = get_datetime_intervals(self.interval_start_date, self.interval_end_date)

        end_dates = [e + dt.timedelta(days=1) for e in end_dates]
        end_dates[-1] = datetime.strptime(self.interval_end_date, YYMD_FORMAT)

        source_start_dates = []
        source_end_dates = []
        target_start_dates = []
        target_end_dates = []

        for start_datetime, end_datetime in zip(start_dates, end_dates):
            end_date = end_datetime.date()
            start_date = previous_weekday_m_days_before(end_date, target_weekday=0, m_days=self.date_duration_days-1)

            target_start_dates.append(start_date)
            target_end_dates.append(end_date)

            source_start_date = start_date - dt.timedelta(days=self.date_period_days)
            source_end_date = end_date - dt.timedelta(days=self.date_period_days)

            source_start_dates.append(source_start_date)
            source_end_dates.append(source_end_date)

        if self.use_datetime:
            return DatetimeInterval(
                source_start_dates=[dt.datetime.fromordinal(d.toordinal()) for d in source_start_dates],
                source_end_dates=[dt.datetime.fromordinal(d.toordinal()) for d in source_end_dates],
                target_start_dates=[dt.datetime.fromordinal(d.toordinal()) for d in target_start_dates],
                target_end_dates=[dt.datetime.fromordinal(d.toordinal()) for d in target_end_dates]
            )
        else:
            return DateInterval(
                source_start_dates=source_start_dates,
                source_end_dates=source_end_dates,
                target_start_dates=target_start_dates,
                target_end_dates=target_end_dates
            )
