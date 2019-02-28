# -*- coding:utf-8 -*-

import os
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import Insert as sa_insert
import openpyxl as xl
from db import DB
import config


USER_COLUMNS = (
    sa.Column("user", sa.VARCHAR(32)),
    sa.Column("password", sa.VARCHAR(256)),
    sa.Column("name", sa.VARCHAR(32)),
    sa.Column("dept", sa.VARCHAR(32)),
    sa.Column("state", sa.VARCHAR(32)),
    sa.PrimaryKeyConstraint("user"),
)
CUSTOMER_COLUMNS = (
    sa.Column("customer", sa.VARCHAR(32)),
    sa.Column("name", sa.VARCHAR(128)),
    sa.Column("type", sa.VARCHAR(32)),
    sa.Column("open_date", sa.DATE),
    sa.PrimaryKeyConstraint("customer"),
)
DEPOSIT_ACCOUNT_COLUMNS = (
    sa.Column("account", sa.VARCHAR(32)),
    sa.Column("customer", sa.VARCHAR(32), ),
    sa.Column("inst", sa.VARCHAR(32)),
    sa.Column("product", sa.VARCHAR(32)),
    sa.Column("open_date", sa.DATE),
    sa.PrimaryKeyConstraint("account"),
    sa.ForeignKeyConstraint(("customer",), ["customer.customer", ]),
)
DEPOSIT_DATA_COLUMNS = (
    sa.Column("account", sa.VARCHAR(32)),
    sa.Column("state", sa.VARCHAR(32)),
    sa.Column("balance", sa.NUMERIC(32, 2, asdecimal=False)),
    sa.Column("month_acc", sa.NUMERIC(32, 2, asdecimal=False)),
    sa.Column("season_acc", sa.NUMERIC(32, 2, asdecimal=False)),
    sa.Column("year_acc", sa.NUMERIC(32, 2, asdecimal=False)),
    sa.Column("date", sa.DATE),
    sa.ForeignKeyConstraint(("account",), ["deposit_account.account", ]),
)
DEPOSIT_OWNER_COLUMNS = (
    sa.Column("customer", sa.VARCHAR(32)),
    sa.Column("user", sa.VARCHAR(32)),
    sa.PrimaryKeyConstraint("customer"),
    sa.ForeignKeyConstraint(("user",), ["user.user", ]),
    sa.ForeignKeyConstraint(("customer",), ["customer.customer", ]),
)


db = DB(
    host=config.IMS_DB_HOST,
    port=config.IMS_DB_PORT,
    user=config.IMS_DB_USER,
    password=config.IMS_DB_PASSWORD,
    name=config.IMS_DB_NAME,
)

if db.ping() is False:
    pass


db.table.register("user", *USER_COLUMNS)
db.table.register("customer", *CUSTOMER_COLUMNS)
db.table.register("deposit_account", *DEPOSIT_ACCOUNT_COLUMNS)
db.table.register("deposit_data", *DEPOSIT_DATA_COLUMNS)
db.table.register("deposit_owner", *DEPOSIT_OWNER_COLUMNS)
db.table.create_all()


DAYS1 = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,)
DAYS2 = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31,)


def days(date):
    if len(date) != 8 or isinstance(date, str) is not True:
        return -1, -1, -1

    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:])

    days_ = DAYS2 if year % 4 == 0 else DAYS1

    m = day
    s = 0
    y = 0

    cur = 1
    while cur < month:
        y += days_[cur]
        cur = cur + 1
    y += day

    cur = month - (month - 1) % 3
    while cur < month:
        s += days_[cur]
        cur = cur + 1
    s += day

    return m, s, y


NIL = 0
NULL = 0
DEMAND = 1
FIX = 2
FIXES = {3: "03", 6: "06", 12: "12", 24: "24", 36: "36", 60: "60", }

INPUTED = 1
UNINPUTED = 2


class SelectDeposit(object):

    def __init__(self, date, scale=10000, precision=2):
        super(SelectDeposit, self).__init__()

        self.date = date

        self.user = db.table("user")
        self.customer = db.table("customer")
        self.account = db.table("deposit_account")
        self.owner = db.table("deposit_owner")

        data = db.table("deposit_data")
        self.data = sa.select([data, ]).where(data.c.date == date).alias("data")

        self.source = self.data.join(self.account, self.data.c.account == self.account.c.account). \
            join(self.customer, self.customer.c.customer == self.account.c.customer). \
            outerjoin(self.owner, self.owner.c.customer == self.customer.c.customer). \
            outerjoin(self.user, self.user.c.user == self.owner.c.user)

        m, s, y = days(date)
        self.balance = sa.func.round(sa.func.sum(self.data.c.balance) / scale, precision)
        self.month_avg = sa.func.round(sa.func.sum(self.data.c.month_acc) / scale / m, precision)
        self.season_avg = sa.func.round(sa.func.sum(self.data.c.season_acc) / scale / s, precision)
        self.year_avg = sa.func.round(sa.func.sum(self.data.c.year_acc) / scale / y, precision)

        self.sql = None

    def where(self, clause):
        self.sql = self.sql.where(clause)
        return self

    def exec(self):
        return db.query(self.sql)

    def product(self, type_, *args):
        if type_ == DEMAND:
            self.sql = self.sql.where(sa.text(self.account.c.product.name + " ~ '%s'" % "^(?!113)"))

        elif type_ == FIX:
            where = "^113"
            if len(args) > 0:
                p = [FIXES[k] for k in args]
                if len(p) > 0:
                    where = where + "\\d{3}(%s)$" % "|".join(p)
            self.sql = self.sql.where(sa.text(self.account.c.product.name + " ~ '%s'" % where))

        return self

    def save_to_excel(self, file, title=None, header=None):
        book = xl.Workbook()
        sheet = book.active

        if title is not None:
            sheet.title = title

        if header is not None:
            sheet.append(header)

        for row in self.exec():
            sheet.append(tuple(row))

        book.save(file)


class SelectUserDeposit(SelectDeposit):

    def __init__(self, date, **kw):
        super(SelectUserDeposit, self).__init__(date, **kw)

        self.sql = self.raw()

    def raw(self):
        return sa.select([
            self.user.c.user, self.user.c.name, self.user.c.dept, self.user.c.state,
            self.balance, self.month_avg, self.season_avg, self.year_avg,
        ]). \
            select_from(self.source). \
            group_by(self.user.c.user)

    def save_to_excel(self, file, title=None, header=None):
        header = header or (
            "用户", "姓名", "部门", "状态", "余额", "月均", "季均", "年均",
        )
        return super().save_to_excel(file, title=title, header=header)


class SelectCustomerDeposit(SelectDeposit):

    def __init__(self, date, **kw):
        super(SelectCustomerDeposit, self).__init__(date, **kw)

        self.sql = self.raw()

    def raw(self):
        return sa.select([
            self.customer.c.customer, self.customer.c.name, self.customer.c.type, self.customer.c.open_date,
            self.user.c.user, self.user.c.name, self.user.c.dept, self.user.c.state,
            self.balance, self.month_avg, self.season_avg, self.year_avg,
        ]). \
            select_from(self.source). \
            group_by(self.customer.c.customer, self.user.c.user)

    def save_to_excel(self, file, title=None, header=None):
        header = header or (
            "客户号", "名称", "类型", "开户日期",
            "用户", "姓名", "部门", "状态",
            "余额", "月均", "季均", "年均",
        )
        return super().save_to_excel(file, title=title, header=header)


class SelectAccountDeposit(SelectDeposit):

    def __init__(self, date, **kw):
        super(SelectAccountDeposit, self).__init__(date, **kw)

        self.sql = self.raw()

    def raw(self):
        return sa.select([
            self.account.c.account, self.account.c.inst, self.account.c.product, self.account.c.open_date,
            self.customer.c.customer, self.customer.c.name, self.customer.c.type, self.customer.c.open_date,
            self.user.c.user, self.user.c.name, self.user.c.dept, self.user.c.state,
            self.balance, self.month_avg, self.season_avg, self.year_avg,
        ]). \
            select_from(self.source). \
            group_by(self.account.c.account, self.customer.c.customer, self.user.c.user)

    def save_to_excel(self, file, title=None, header=None):
        header = header or (
            "账号", "机构", "产品", "开户日期1",
            "客户号", "名称", "类型", "开户日期2",
            "用户", "姓名", "部门", "状态",
            "余额", "月均", "季均", "年均",
        )
        return super().save_to_excel(file, title=title, header=header)


class SelectInstDeposit(SelectDeposit):

    def __init__(self, date, **kw):
        super(SelectInstDeposit, self).__init__(date, **kw)

        self.sql = self.raw()

    def raw(self):
        return sa.select([
            self.account.c.inst,
            self.balance, self.month_avg, self.season_avg, self.year_avg,
        ]). \
            select_from(self.source). \
            group_by(self.account.c.inst)

    def save_to_excel(self, file, title=None, header=None):
        header = header or (
            "账号", "余额", "月均", "季均", "年均",
        )
        return super().save_to_excel(file, title=title, header=header)


def combine_deposit(demand, fix):
    res = {}

    for row in demand:
        key = row[0]
        r = []
        r.extend((
            *row[0: -4],
            float(row[-4]), float(row[-3]), float(row[-2]), float(row[-1]),
            0.00, 0.00, 0.00, 0.00,
            float(row[-4]), float(row[-3]), float(row[-2]), float(row[-1]),))
        res[key] = r

    for row in fix:
        key = row[0]
        if key in res:
            r = res[key]
            r[-8] = float(row[-4])
            r[-7] = float(row[-3])
            r[-6] = float(row[-2])
            r[-5] = float(row[-1])
            r[-4] = r[-12] + r[-8]
            r[-3] = r[-11] + r[-7]
            r[-2] = r[-10] + r[-6]
            r[-1] = r[-9] + r[-5]

        else:
            r = []
            r.extend(
                (*row[0: -4],
                 0.00, 0.00, 0.00, 0.00,
                 float(row[-4]), float(row[-3]), float(row[-2]), float(row[-1]),
                 float(row[-4]), float(row[-3]), float(row[-2]), float(row[-1]),))
            res[key] = r

    return res


def save_user_deposit(date, file, title=None):
    res = combine_deposit(
        SelectUserDeposit(date).product(DEMAND).exec(),
        SelectUserDeposit(date).product(FIX).exec()
    )

    book = xl.load_workbook(os.path.join(config.TEMPLATE_DIR, "deposit", "user.xlsx"))
    sheet = book.active
    if title is not None:
        sheet.title = title

    for row in res.values():
        sheet.append(row)

    book.save(file)


def save_customer_deposit(date, file, title=None):
    res = combine_deposit(
        SelectCustomerDeposit(date).product(DEMAND).exec(),
        SelectCustomerDeposit(date).product(FIX).exec()
    )

    book = xl.load_workbook(os.path.join(config.TEMPLATE_DIR, "deposit", "customer.xlsx"))
    sheet = book.active
    if title is not None:
        sheet.title = title

    for row in res.values():
        sheet.append(row)

    book.save(file)


def save_account_deposit(date, file, title=None):
    sql = SelectAccountDeposit(date)

    book = xl.load_workbook(os.path.join(config.TEMPLATE_DIR, "deposit", "account.xlsx"))
    sheet = book.active
    if title is not None:
        sheet.title = title

    for row in sql.exec():
        sheet.append(row.values())

    book.save(file)


def save_inst_deposit(date, file, title=None, inputed=NIL):
    demand = SelectInstDeposit(date).product(DEMAND)
    fix = SelectInstDeposit(date).product(FIX)

    if inputed == INPUTED:
        demand.where(demand.user.c.user != None)
        fix.where(fix.user.c.user != None)
    elif inputed == UNINPUTED:
        demand.where(demand.user.c.user == None)
        fix.where(fix.user.c.user == None)

    res = combine_deposit(
        demand.exec(),
        fix.exec()
    )

    book = xl.load_workbook(os.path.join(config.TEMPLATE_DIR, "deposit", "inst.xlsx"))
    sheet = book.active
    if title is not None:
        sheet.title = title

    for row in res.values():
        sheet.append(row)

    book.save(file)


class Insert(object):

    def __init__(self, t):
        super(Insert, self).__init__()

        self.t = t
        self.sql = sa_insert(self.t)

    def do_update(self):
        set_ = {}
        for c in self.t.c:
            set_[c.name] = getattr(self.sql.excluded, c.name)

        self.sql = self.sql.on_conflict_do_update(
            index_elements=self.t.primary_key,
            set_=set_
        )
        return self

    def do_nothing(self):
        self.sql = self.sql.on_conflict_do_nothing(
            index_elements=self.t.primary_key
        )
        return self

    def exec(self, *args, **kw):
        return db.commit(self.sql, *args, **kw)


class ImportUser(object):

    def __init__(self):
        super(ImportUser, self).__init__()

        self.sql = Insert(db.table("user"))

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

    def __init__(self):
        super(ImportCustomer, self).__init__()

        self.sql = Insert(db.table("customer"))

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

    def __init__(self):
        super(ImportDepositAccount, self).__init__()

        self.sql = Insert(db.table("deposit_account"))

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

    def __init__(self):
        super(ImportDepositData, self).__init__()

        self.sql = Insert(db.table("deposit_data"))

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

    def __init__(self):
        super(ImportDepositOwner, self).__init__()

        self.sql = Insert(db.table("deposit_owner"))

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


def import_all(dir_):
    files = os.listdir(dir_)

    for f in files:
        if f.startswith("CUS"):
            im = ImportCustomer()
            im.sql.do_nothing()
            im.from_txt(os.path.join(dir_, f))
        elif f.startswith("DEP"):
            im = ImportDepositAccount()
            im.sql.do_nothing()
            im.from_txt(os.path.join(dir_, f))

            im = ImportDepositData()
            im.from_txt(os.path.join(dir_, f))


if __name__ == "__main__":

    date = "20190226"
    save_user_deposit(date, "D:/Desktop/1.xlsx")

