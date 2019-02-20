# -*-coding:utf-8 -*-

import os

from model import utils

DOWNLOAD_DIR = "./download/"
EXPORT_DIR = "D:/Desktop/"


def go(date):
    download_dir = os.path.join(DOWNLOAD_DIR, date)

    #task.download("20190213")

    files = os.listdir(download_dir)
    for f in files:
        path = os.path.join(download_dir, f)

        if f.startswith("CUS"):
            action = utils.ImportCustomer()
            action.sql.do_nothing()
            action.from_txt(path)

        elif f.startswith("INST"):
            action = utils.ImportDepositAccount()
            action.sql.do_nothing()
            action.from_txt(path)

            action = utils.ImportDepositData()
            action.from_txt(path)


if __name__ == "__main__":
    date = "20181231"
    meta = utils.MetaDeposit(date)
    action = utils.ExportUserDeposit(meta)
    action.save_to_excel(
        os.path.join(EXPORT_DIR, "USER-" + date + ".xlsx"),
        template="./template/deposit/user.xlsx", )
    action = utils.ExportInstDeposit(date)
    action.save_to_excel(
        os.path.join(EXPORT_DIR, "INST-" + date + ".xlsx"),
        template="./template/deposit/inst.xlsx",)



