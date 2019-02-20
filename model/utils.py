# -*- coding:utf8 -*-

from collections import OrderedDict

from .sql import *
from . import xl


DEMAND_BALANCE = "demand_balance"
DEMAND_MONTH_AVG = "demand_month_avg"
DEMAND_SEASON_AVG = "demand_season_avg"
DEMAND_YEAR_AVG = "demand_year_avg"

FIX_BALANCE = "fix_balance"
FIX_MONTH_AVG = "fix_month_avg"
FIX_SEASON_AVG = "fix_season_avg"
FIX_YEAR_AVG = "fix_year_avg"

SUM_BALANCE = "sum_balance"
SUM_MONTH_AVG = "sum_month_avg"
SUM_SEASON_AVG = "sum_season_avg"
SUM_YEAR_AVG = "sum_year_avg"


class ExportDeposit(object):

    def __init__(self, date, scale=10000, precision=2):
        super(ExportDeposit, self).__init__()

        self.user = table.User()
        self.customer = table.Customer()
        self.account = table.DepositAccount()
        self.data = table.DepositData.from_date(date)
        self.owner = table.DepositOwner()

        self.source = self.data.join(self.account, self.data.c.account == self.account.c.account). \
            join(self.customer, self.customer.c.customer == self.account.c.customer). \
            outerjoin(self.owner, self.owner.c.customer == self.customer.c.customer). \
            outerjoin(self.user, self.user.c.user == self.owner.c.user)

        m, s, y = days.days(date)
        self.balance = sa.func.round(sa.func.sum(self.data.c.balance) / scale, precision)
        self.month_avg = sa.func.round(sa.func.sum(self.data.c.month_acc) / scale / m, precision)
        self.season_avg = sa.func.round(sa.func.sum(self.data.c.season_acc) / scale / s, precision)
        self.year_avg = sa.func.round(sa.func.sum(self.data.c.year_acc) / scale / y, precision)

        self.sql = None

    def product(self, type_, *args):
        if type_ == DEMAND:
            return sa.text(self.account.c.product.name + " ~ '%s'" % "^(?!113)")

        elif type_ == FIX:
            where = "^113"
            if len(args) > 0:
                p = [FIXES[k] for k in args]
                if len(p) > 0:
                    where = where + "\\d{3}(%s)$" % "|".join(p)
            return sa.text(self.account.c.product.name + " ~ '%s'" % where)

    def result(self):
        demand = db.query(self.sql.where(self.product(DEMAND)))
        fix = db.query(self.sql.where(self.product(FIX)))
        return demand, fix


class ExportDepositByUser(ExportDeposit):

    def __init__(self, date, **kw):
        super(ExportDepositByUser, self).__init__(date, **kw)

        self.sql = sa.select([
            self.user.c.user.label("user"),
            self.user.c.name.label("name"),
            self.user.c.dept.label("dept"),
            self.user.c.state.label("state"),
            self.balance.label("balance"),
            self.month_avg.label("month_avg"),
            self.season_avg.label("season_avg"),
            self.year_avg.label("year_avg"),
        ]). \
            select_from(self.source). \
            group_by(self.user.c.user)

    def init_row(self, row):
        od = OrderedDict()

        od["user"] = row["user"]
        od["name"] = row["name"]
        od["dept"] = row["dept"]
        od["state"] = row["state"]

        od[DEMAND_BALANCE] = 0.00
        od[DEMAND_MONTH_AVG] = 0.00
        od[DEMAND_SEASON_AVG] = 0.00
        od[DEMAND_YEAR_AVG] = 0.00

        od[FIX_BALANCE] = 0.00
        od[FIX_MONTH_AVG] = 0.00
        od[FIX_SEASON_AVG] = 0.00
        od[FIX_YEAR_AVG] = 0.00

        od[SUM_BALANCE] = 0.00
        od[SUM_MONTH_AVG] = 0.00
        od[SUM_SEASON_AVG] = 0.00
        od[SUM_YEAR_AVG] = 0.00

        return od

    def _combine_res(self, demand, fix):
        res = {}

        for row in demand:
            user = row["user"]
            if user not in res:
                res[user] = self.init_row(row)
            item = res[user]
            item[DEMAND_BALANCE] = float(row["balance"])
            item[DEMAND_MONTH_AVG] = float(row["month_avg"])
            item[DEMAND_SEASON_AVG] = float(row["season_avg"])
            item[DEMAND_YEAR_AVG] = float(row["year_avg"])

        for row in fix:
            user = row["user"]
            if user not in res:
                res[user] = self.init_row(row)
            item = res[user]
            item[FIX_BALANCE] = float(row["balance"])
            item[FIX_MONTH_AVG] = float(row["month_avg"])
            item[FIX_SEASON_AVG] = float(row["season_avg"])
            item[FIX_YEAR_AVG] = float(row["year_avg"])

        for v in res.values():
            v[SUM_BALANCE] = v[DEMAND_BALANCE] + v[FIX_BALANCE]
            v[SUM_MONTH_AVG] = v[DEMAND_MONTH_AVG] + v[FIX_MONTH_AVG]
            v[SUM_SEASON_AVG] = v[DEMAND_SEASON_AVG] + v[FIX_SEASON_AVG]
            v[SUM_YEAR_AVG] = v[DEMAND_YEAR_AVG] + v[FIX_YEAR_AVG]

        return res

    def save_to_excel(self, file, sheet=None, template=None):
        if template is None:
            template = "./template/deposit/user.xlsx"

        demand, fix = self.result()
        res = self._combine_res(demand, fix)

        wb = xl.load_workbook(template)
        ws = wb.active
        if sheet is not None:
            ws.title = sheet

        for row in res.values():
            ws.append(list(row.values()))

        wb.save(file)


class ExportDepositByInst(ExportDeposit):

    def __init__(self, date, **kw):
        super(ExportDepositByInst, self).__init__(date, **kw)

        self.sql = sa.select([
            self.account.c.inst.label("inst"),
            self.balance.label("balance"),
            self.month_avg.label("month_avg"),
            self.season_avg.label("season_avg"),
            self.year_avg.label("year_avg"),
        ]).\
            select_from(self.source).\
            group_by(self.account.c.inst)

    def init_row(self, row):
        od = OrderedDict()

        od["inst"] = row["inst"]
        od["name"] = ""
        od["dept"] = ""
        od["state"] = ""

        od[DEMAND_BALANCE] = 0.00
        od[DEMAND_MONTH_AVG] = 0.00
        od[DEMAND_SEASON_AVG] = 0.00
        od[DEMAND_YEAR_AVG] = 0.00

        od[FIX_BALANCE] = 0.00
        od[FIX_MONTH_AVG] = 0.00
        od[FIX_SEASON_AVG] = 0.00
        od[FIX_YEAR_AVG] = 0.00

        od[SUM_BALANCE] = 0.00
        od[SUM_MONTH_AVG] = 0.00
        od[SUM_SEASON_AVG] = 0.00
        od[SUM_YEAR_AVG] = 0.00

        return od

    def _combine_res(self, demand, fix):
        res = {}

        for row in demand:
            user = row["inst"]
            if user not in res:
                res[user] = self.init_row(row)
            item = res[user]
            item[DEMAND_BALANCE] = float(row["balance"])
            item[DEMAND_MONTH_AVG] = float(row["month_avg"])
            item[DEMAND_SEASON_AVG] = float(row["season_avg"])
            item[DEMAND_YEAR_AVG] = float(row["year_avg"])

        for row in fix:
            user = row["inst"]
            if user not in res:
                res[user] = self.init_row(row)
            item = res[user]
            item[FIX_BALANCE] = float(row["balance"])
            item[FIX_MONTH_AVG] = float(row["month_avg"])
            item[FIX_SEASON_AVG] = float(row["season_avg"])
            item[FIX_YEAR_AVG] = float(row["year_avg"])

        for v in res.values():
            v[SUM_BALANCE] = v[DEMAND_BALANCE] + v[FIX_BALANCE]
            v[SUM_MONTH_AVG] = v[DEMAND_MONTH_AVG] + v[FIX_MONTH_AVG]
            v[SUM_SEASON_AVG] = v[DEMAND_SEASON_AVG] + v[FIX_SEASON_AVG]
            v[SUM_YEAR_AVG] = v[DEMAND_YEAR_AVG] + v[FIX_YEAR_AVG]

        return res

    def save_to_excel(self, path, sheet=None, template=None):
        if template is None:
            template = "./template/deposit/inst.xlsx"

        sql = self.meta.select_deposit_by_inst

        demand = self.meta.product(DEMAND)
        fix = self.meta.product(FIX)
        res = self._combine_res(
            db.query(sql.where(demand)),
            db.query(sql.where(fix))
        )

        wb = xl.load_workbook(template)
        ws = wb.active
        if sheet is not None:
            ws.title = sheet

        for row in res.values():
            ws.append(list(row.values()))

        wb.save(path)


class ImportUser(object):

    def __init__(self, name=None):
        super(ImportUser, self).__init__()
        self.sql = Insert(table.User(name))

    def from_excel(self, file, sheet=None):
        wb = xl.load_workbook(file)
        ws = wb.active if sheet is None else wb[sheet]
        content = []
        for row in ws.iter_rows(min_row=2):
            content.append(
                {
                    "user": row[0].value,
                    "password": row[1].value,
                    "name": row[2].value,
                    "dept": row[3].value,
                    "state": row[4].value,
                }
            )
        wb.close()
        return self.sql.exec(*content)


class ImportCustomer(object):

    def __init__(self, name=None):
        super(ImportCustomer, self).__init__()
        self.sql = Insert(table.Customer(name))

    def from_excel(self, file, sheet=None):
        wb = xl.load_workbook(file)
        ws = wb.active if sheet is None else wb[sheet]
        content = []
        for row in ws.iter_rows(min_row=2):
            content.append(
                {
                    "customer": row[4].value[-11:],
                    "name": row[5].value,
                    "type": row[9].value,
                    "open_date": row[10].value,
                }
            )
        wb.close()
        return self.sql.exec(*content)

    def from_txt(self, file, delimiter=","):
        content = []
        with open(file, encoding="utf8") as f:
            lines = f.readlines()
            if len(lines) <= 1:
                return
            for line in lines[1:]:
                row = line.split(delimiter)
                content.append({
                    "customer": row[4].replace(" ", "")[-11:],
                    "name": row[5].replace(" ", ""),
                    "type": row[9].replace(" ", ""),
                    "open_date": row[10].replace(" ", ""),
                })

        return self.sql.exec(*content)


class ImportDepositAccount(object):

    def __init__(self, name=None):
        super(ImportDepositAccount, self).__init__()
        self.sql = Insert(table.DepositAccount(name))

    def from_excel(self, file, sheet=None):
        wb = xl.load_workbook(file)
        ws = wb.active if sheet is None else wb[sheet]
        content = []
        for row in ws.iter_rows(min_row=2):
            content.append(
                {
                    "account": row[3].value,
                    "customer": row[0].value[-11:],
                    "inst": row[1].value,
                    "product": row[7].value,
                    "open_date": row[9].value,
                }
            )
        wb.close()
        return self.sql.exec(*content)

    def from_txt(self, file, delimiter=","):
        content = []
        with open(file, encoding="utf8") as f:
            lines = f.readlines()
            for line in lines[1:]:
                row = line.split(delimiter)
                content.append({
                    "account": row[3].replace(" ", ""),
                    "customer": row[0].replace(" ", "")[-11:],
                    "inst": row[1].replace(" ", ""),
                    "product": row[7].replace(" ", ""),
                    "open_date": row[9].replace(" ", ""),
                })

        return self.sql.exec(*content)


class ImportDepositData(object):

    def __init__(self, name=None):
        super(ImportDepositData, self).__init__()
        self.sql = Insert(table.DepositData(name))

    def from_excel(self, file, sheet=None):
        wb = xl.load_workbook(file)
        ws = wb.active if sheet is None else wb[sheet]
        content = []
        for row in ws.iter_rows(min_row=2):
            content.append(
                {
                    "account": row[3].value,
                    "state": row[11].value,
                    "balance": row[13].value,
                    "month_acc": row[14].value,
                    "season_acc": row[15].value,
                    "year_acc": row[16].value,
                    "date": row[17].value,
                }
            )
        wb.close()
        return self.sql.exec(*content)

    def from_txt(self, file, delimiter=","):
        content = []
        with open(file, encoding="utf8") as f:
            lines = f.readlines()
            for line in lines[1:]:
                row = line.split(delimiter)
                content.append({
                    "account": row[3].replace(" ", ""),
                    "state": row[11].replace(" ", ""),
                    "balance": row[13].replace(" ", ""),
                    "month_acc": row[14].replace(" ", ""),
                    "season_acc": row[15].replace(" ", ""),
                    "year_acc": row[16].replace(" ", ""),
                    "date": row[17].replace(" ", ""),
                })

        return self.sql.exec(*content)


class ImportDepositOwner(object):

    def __init__(self, name=None):
        super(ImportDepositOwner, self).__init__()
        self.sql = Insert(table.DepositOwner(name))

    def from_excel(self, file, sheet=None):
        wb = xl.load_workbook(file)
        ws = wb.active if sheet is None else wb[sheet]
        content = []
        for row in ws.iter_rows(min_row=2):
            content.append(
                {
                    "customer": row[0].value[-11:],
                    "user": row[1].value,
                }
            )
        wb.close()
        return self.sql.exec(*content)
