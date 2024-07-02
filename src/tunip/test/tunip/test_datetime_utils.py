import datetime as dt
from tunip.datetime_utils import previous_sunday, previous_weekday_m_days_before

# previous_weekday_m_days_before(x, target_weekday=0, m_days=7)
# previous_weekday_m_days_before(x, target_weekday=0, m_days=7)

# def get_date_interval_end(date):
#     prev_sunday = previous_sunday(date)
#     return prev_sunday

# previous_sunday(dt.datetime(2023, 7, 25))

def previous_weekday_m_days_before2(target_date, target_weekday, m_days=30):
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


target_weekday = 6
m_days = 7
(
    previous_weekday_m_days_before2(dt.datetime(2023, 7, 23), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 7, 24), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 7, 25), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 7, 26), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 7, 27), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 7, 29), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 7, 30), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 7, 31), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 8, 1), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 8, 2), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 8, 3), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 8, 4), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 8, 5), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 8, 6), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 8, 7), target_weekday=target_weekday, m_days=m_days),
    previous_weekday_m_days_before2(dt.datetime(2023, 8, 8), target_weekday=target_weekday, m_days=m_days),
)