# -*- coding:utf-8 -*-


__day1__ = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,)
__day2__ = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,)


def days(date):
    if len(date) != 8 or isinstance(date, str) is not True:
        return -1, -1, -1

    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:])

    days = __day2__ if year % 4 == 0 else __day1__

    m = day
    s = 0
    y = 0

    cur = 1
    while cur < month:
        y += days[cur]
        cur = cur + 1
    y += day

    cur = month - (month - 1) % 3
    while cur < month:
        s += days[cur]
        cur = cur + 1
    s += day

    return m, s, y

