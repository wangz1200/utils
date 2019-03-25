# -*- coding:utf-8 -*-

import os
import datetime
import ims
from crawler import jm


def job():
    date = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y%m%d")

    out = os.path.join("D:/Desktop/repo/out", date)
    os.makedirs(out, exist_ok=True)

    jm.download_all(date, out)
    ims.import_all(out, date)


if __name__ == "__main__":
    job()
