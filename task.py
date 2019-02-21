# -*- coding:utf-8 -*-

import os
import ims
from crawler import jmreport

DIR = "D:/Desktop"


if __name__ == "__main__":
    date = "20190219"

    dir = os.path.join(DIR, date)
    os.makedirs(dir, exist_ok=True)

    jmreport.download_all(date, dir)
    db = ims.init()
    ims.import_all(db, dir)
    ims.export_all_deposit(db, date, dir)



