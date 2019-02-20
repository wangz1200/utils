# -*-  coding: utf-8 -*-

import os

import model
from report.jmreport import *


DOWNLOAD_DIR = "./download/"
EXPORT_DIR = "./export/"


def download(dir, date):

    jr = JmReport()
    jr.login().init()

    cus = jr.new_customer_report()
    cus.init()

    path = os.path.join(dir, "CUS-" + date + ".txt")
    cus.set_date(date)
    cus.download(path)

    dep = jr.new_deposit_report().init()

    dep.list_date()
    dep.set_date(date)

    dep.list_root_inst()
    dep.list_child_inst("70300")

    for inst in ("70315", "70317", "70318", "70319"):
        dep.set_inst(inst)
        path = os.path.join(dir, "INST-" + inst + "-" + date + ".txt")
        dep.download(path)

    jr.close()



