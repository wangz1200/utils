# -*- coding:utf-8 -*-

import os
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import Insert as sa_insert
import openpyxl as xl
from db import DB
import config


db = DB(
    host=config.IMS_DB_HOST,
    port=config.IMS_DB_PORT,
    user=config.IMS_DB_USER,
    password=config.IMS_DB_PASSWORD,
    name=config.IMS_DB_NAME,
)

if db.ping() is False:
    pass


def create_table(name, *columns):

    t = db.t(name)
    if t is None:
        t = db.t.create(name, *columns)

    return t


def create_users_table(name=None):

    return db.t.create(
        name or "users",
        sa.Column("u", sa.VARCHAR(32)),
        sa.Column("password", sa.VARCHAR(256)),
        sa.Column("name", sa.VARCHAR(32)),
        sa.Column("dept", sa.VARCHAR(32)),
        sa.Column("state", sa.VARCHAR(32)),
        sa.PrimaryKeyConstraint("u"),
    )


def create_cust_table(name=None):

    return db.t.create(
        name or "cust",
        sa.Column("cust", sa.VARCHAR(32)),
        sa.Column("name", sa.VARCHAR(128)),
        sa.Column("type", sa.VARCHAR(32)),
        sa.Column("open_date", sa.DATE),
        sa.PrimaryKeyConstraint("cust"),
    )


def create_dep_acct_table(name=None):

    return db.t.create(
        name or "dep_acct",
        sa.Column("acct", sa.VARCHAR(32)),
        sa.Column("cust", sa.VARCHAR(32), ),
        sa.Column("inst", sa.VARCHAR(32)),
        sa.Column("prod", sa.VARCHAR(32)),
        sa.Column("open_date", sa.DATE),
        sa.PrimaryKeyConstraint("acct"),
        sa.ForeignKeyConstraint(("cust",), ["cust.cust", ]),
    )


def create_dep_data_table(name=None):

    return db.t.create(
        name or "dep_data",
        sa.Column("acct", sa.VARCHAR(32)),
        sa.Column("balance", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("month_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("season_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("year_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("date", sa.DATE),
        sa.ForeignKeyConstraint(("acct",), ["dep_acct.acct", ]),
    )


def create_dep_cust_owner_table(name=None):

    return db.t.create(
        name or "dep_cust_owner",
        sa.Column("cust", sa.VARCHAR(32)),
        sa.Column("u", sa.VARCHAR(32)),
        sa.PrimaryKeyConstraint("cust"),
        sa.ForeignKeyConstraint(("cust",), ["cust.cust", ]),
        sa.ForeignKeyConstraint(("u",), ["users.u", ]),
    )


def create_dep_acct_owner_table(name=None):

    return db.t.create(
        name or "dep_acct_owner",
        sa.Column("acct", sa.VARCHAR(32)),
        sa.Column("u", sa.VARCHAR(32)),
        sa.PrimaryKeyConstraint("acct"),
        sa.ForeignKeyConstraint(("acct",), ["dep_acct.acct", ]),
        sa.ForeignKeyConstraint(("u",), ["users.u", ]),
    )


def create_dep_owner_table(name=None):

    return db.t.create(
        name or "dep_owner",
        sa.Column("acct", sa.VARCHAR(32)),
        sa.Column("u", sa.VARCHAR(32)),
        sa.PrimaryKeyConstraint("acct"),
        sa.ForeignKeyConstraint(("acct",), ["dep_acct.acct", ]),
        sa.ForeignKeyConstraint(("u",), ["users.u", ]),
    )


create_users_table()
create_cust_table()
create_dep_acct_table()
create_dep_cust_owner_table()
create_dep_acct_owner_table()
create_dep_owner_table()


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


FIXES = {3: "03", 6: "06", 12: "12", 24: "24", 36: "36", 60: "60", }


class _SelectDep(object):

    def __init__(self, date, scale=10000, precision=2):

        super(_SelectDep, self).__init__()

        self.users = db.table("users")
        self.cust = db.table("cust")
        self.acct = db.table("dep_acct")

        data = create_dep_data_table("dep_data_" + date[0:4])
        self.data = sa.select([data, ]).where(data.c.date == date).alias("data")

        m, s, y = days(date)
        self.balance = sa.func.round(sa.func.sum(self.data.c.balance) / scale, precision)
        self.month_avg = sa.func.round(sa.func.sum(self.data.c.month_acc) / scale / m, precision)
        self.season_avg = sa.func.round(sa.func.sum(self.data.c.season_acc) / scale / s, precision)
        self.year_avg = sa.func.round(sa.func.sum(self.data.c.year_acc) / scale / y, precision)

        self.sql = None

    def demand(self):
        return self.sql.where(sa.text(self.acct.c.prod.name + " ~ '%s'" % "^(?!113)"))

    def fix(self, *args):
        where = "^113"
        if len(args) > 0:
            p = [FIXES[k] for k in args]
            if len(p) > 0:
                where = where + "\\d{3}(%s)$" % "|".join(p)
        return self.sql.where(sa.text(self.acct.c.prod.name + " ~ '%s'" % where))


class SelectUserDep(_SelectDep):

    HEADER = ("用户", "姓名", "部门", "状态", "余额", "月均", "季均", "年均", )

    def __init__(self, date, **kw):

        super(SelectUserDep, self).__init__(date, **kw)

        self.owner = db.table("dep_owner")

        self.source = self.data.join(self.acct, self.data.c.acct == self.acct.c.acct). \
            outerjoin(self.owner, self.owner.c.acct == self.acct.c.acct). \
            outerjoin(self.users, self.users.c.u == self.owner.c.u)

        self.sql = sa.select([
            self.users.c.u,
            self.users.c.name,
            self.users.c.dept,
            self.users.c.state,
            self.balance,
            self.month_avg,
            self.season_avg,
            self.year_avg
        ]).\
            select_from(self.source).\
            group_by(self.users.c.u)


class SelectCustDep(_SelectDep):

    HEADER = ("客户号", "客户名称", "客户类型", "开户日期", "用户", "姓名", "部门", "状态", "余额", "月均", "季均", "年均", )

    def __init__(self, date, **kw):

        super(SelectCustDep, self).__init__(date, **kw)

        self.owner = db.table("dep_cust_owner")

        self.source = self.data.join(self.acct, self.data.c.acct == self.acct.c.acct). \
            join(self.cust, self.cust.c.cust == self.acct.c.cust). \
            outerjoin(self.owner, self.owner.c.cust == self.acct.c.cust). \
            outerjoin(self.users, self.users.c.u == self.owner.c.u)

        self.sql = sa.select([
            self.cust.c.cust,
            self.cust.c.name,
            self.cust.c.type,
            self.cust.c.open_date,
            self.users.c.u,
            self.users.c.name,
            self.users.c.dept,
            self.users.c.state,
            self.balance,
            self.month_avg,
            self.season_avg,
            self.year_avg
        ]).\
            select_from(self.source).\
            group_by(self.cust.c.cust, self.users.c.u)


class SelectAcctDep(_SelectDep):

    HEADER = ("账户", "机构", "产品", "开户日期", "客户号", "客户名称", "类型", "开户日期2", "用户", "姓名", "部门", "机构", "余额", "月均", "季均", "年均", )

    def __init__(self, date, **kw):

        super(SelectAcctDep, self).__init__(date, **kw)

        self.owner = db.table("dep_owner")

        self.source = self.data.join(self.acct, self.data.c.acct == self.acct.c.acct). \
            join(self.cust, self.cust.c.cust == self.acct.c.cust). \
            outerjoin(self.owner, self.owner.c.acct == self.acct.c.acct). \
            outerjoin(self.users, self.users.c.u == self.owner.c.u)

        self.sql = sa.select([
            self.acct.c.acct,
            self.acct.c.inst,
            self.acct.c.prod,
            self.acct.c.open_date,
            self.cust.c.cust,
            self.cust.c.name,
            self.cust.c.type,
            self.cust.c.open_date,
            self.users.c.u,
            self.users.c.name,
            self.users.c.dept,
            self.users.c.state,
            self.balance,
            self.month_avg,
            self.season_avg,
            self.year_avg
        ]). \
            select_from(self.source). \
            group_by(self.acct.c.acct, self.cust.c.cust, self.users.c.u)


class SelectInstDep(_SelectDep):

    HEADER = ("机构", "余额", "月均", "季均", "年均", )

    def __init__(self, date, **kw):

        super(SelectInstDep, self).__init__(date, **kw)

        self.owner = db.table("dep_owner")

        self.source = self.data.join(self.acct, self.data.c.acct == self.acct.c.acct). \
            join(self.cust, self.cust.c.cust == self.acct.c.cust). \
            outerjoin(self.owner, self.owner.c.acct == self.acct.c.acct). \
            outerjoin(self.users, self.users.c.u == self.owner.c.u)

        self.sql = sa.select([
            self.acct.c.inst,
            self.balance,
            self.month_avg,
            self.season_avg,
            self.year_avg
        ]). \
            select_from(self.source). \
            group_by(self.acct.c.inst)


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


def save_dep_to_excel(sql, file, title=None, header=None):
    book = xl.Workbook()
    sheet = book.active

    if title is not None:
        sheet.title = title

    if header is not None:
        sheet.append(header)

    for row in db.query(sql):
        sheet.append(tuple(row))

    book.save(file)


def _save_dep_with_tmpl(dist, tmpl, rows, title=None):

    book = xl.load_workbook(tmpl)
    sheet = book.active

    if title is not None:
        sheet.title = title

    for row in rows:
        sheet.append(tuple(row))

    book.save(dist)


def save_user_dep_to_excel(date, file, title=None):

    sel = SelectUserDep(date)

    res = combine_deposit(
        db.query(sel.demand()),
        db.query(sel.fix())
    )

    _save_dep_with_tmpl(
        file,
        os.path.join(config.TEMPLATE_DIR, "deposit", "user.xlsx"),
        res.values(),
        title=title
    )


def save_cust_dep_to_excel(date, file, title=None):

    sel = SelectCustDep(date)

    res = combine_deposit(
        db.query(sel.demand()),
        db.query(sel.fix())
    )

    _save_dep_with_tmpl(
        file,
        os.path.join(config.TEMPLATE_DIR, "deposit", "customer.xlsx"),
        res.values(),
        title=title
    )


def save_acct_dep_to_excel(date, file, title=None):

    sel = SelectAcctDep(date)

    _save_dep_with_tmpl(
        file,
        os.path.join(config.TEMPLATE_DIR, "deposit", "account.xlsx"),
        db.query(sel.sql),
        title=title
    )


def save_inst_dep_to_excel(date, file, title=None):

    sel = SelectInstDep(date)

    res = combine_deposit(
        db.query(sel.demand().where(sel.users.c.u == None)),
        db.query(sel.fix().where(sel.users.c.u == None))
    )

    _save_dep_with_tmpl(
        file,
        os.path.join(config.TEMPLATE_DIR, "deposit", "inst.xlsx"),
        res.values(),
        title=title
    )


def insert(t):
    return sa_insert(t)


def insert_with_update(t):
    stmt = sa_insert(t)
    set_ = {}
    for c in t.c:
        set_[c.name] = getattr(stmt.excluded, c.name)
    stmt = stmt.on_conflict_do_update(
        index_elements=stmt.t.primary_key,
        set_=set_
    )
    return stmt


def insert_with_nothing(t):
    stmt = sa_insert(t)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=t.primary_key
    )
    return stmt


def insert_users_from_txt(file, with_update=False, delimter=","):
    pass


def insert_cust_from_txt(file, with_update=False, delimiter=","):
    content = []
    with open(file, encoding="utf8") as f:
        lines = f.readlines()
        if len(lines) <= 1:
            return
        for line in lines[1:]:
            row = line.split(delimiter)
            content.append({
                "cust": row[4].replace(" ", "")[-11:],
                "name": row[5].replace(" ", ""),
                "type": row[9].replace(" ", ""),
                "open_date": row[10].replace(" ", ""),
            })
    t = db.t("cust")
    if with_update:
        stmt = insert_with_update(t)
    else:
        stmt = insert_with_nothing(t)
    return db.commit(stmt, *content)


def insert_dep_acct_from_txt(file, with_update=False, delimiter=","):
    content = []
    with open(file, encoding="utf8") as f:
        lines = f.readlines()
        for line in lines[1:]:
            row = line.split(delimiter)
            content.append({
                "acct": row[3].replace(" ", ""),
                "cust": row[0].replace(" ", "")[-11:],
                "inst": row[1].replace(" ", ""),
                "prod": row[7].replace(" ", ""),
                "open_date": row[9].replace(" ", ""),
            })
    t = db.t("dep_acct")
    if with_update:
        stmt = insert_with_update(t)
    else:
        stmt = insert_with_nothing(t)
    return db.commit(stmt, *content)


def insert_dep_data_from_txt(date, file, delimiter=","):
    content = []
    with open(file, encoding="utf8") as f:
        lines = f.readlines()
        for line in lines[1:]:
            row = line.split(delimiter)
            content.append({
                "acct": row[3].replace(" ", ""),
                "balance": row[14].replace(" ", ""),
                "month_acc": row[15].replace(" ", ""),
                "season_acc": row[17].replace(" ", ""),
                "year_acc": row[16].replace(" ", ""),
                "date": row[18].replace("\n", ""),
            })
    t = create_dep_data_table("dep_data_" + date[0:4])
    stmt = sa_insert(t)
    return db.commit(stmt, *content)


def import_all(dir_, date, with_update=False):

    files = os.listdir(dir_)

    for f in files:
        if f.startswith("CUS"):
            ret = insert_cust_from_txt(os.path.join(dir_, f), with_update=with_update)
            print("Insert Cust --- %s:" % f, " Result: ", ret)
        elif f.startswith("DEP"):
            ret = insert_dep_acct_from_txt(os.path.join(dir_, f), with_update=with_update)
            print("Insert Dep Acct --- %s:" % f, " Result: ", ret)
            ret = insert_dep_data_from_txt(date, os.path.join(dir_, f))
            print("Insert Dep Data --- %s:" % f, " Result: ", ret)


def go():

    #im = ImportDepositOwner()
    #im.sql.do_nothing()
    #im.from_excel("D:/Desktop/repo/owner/OWNER.xlsx")

    date = "20190323"
    save_user_dep_to_excel(date, "D:/Desktop/USER-" + date + ".xlsx")
    save_cust_dep_to_excel(date, "D:/Desktop/CUST-" + date + ".xlsx")
    save_acct_dep_to_excel(date, "D:/Desktop/ACCT-" + date + ".xlsx")
    save_inst_dep_to_excel(date, "D:/Desktop/INST-" + date + ".xlsx")

    #sel = SelectInstDeposit(date)
    #sel.product(FIX, 12, 24, 36, 60)
    #sel.where(sel.user.c.user == None)
    #sel.save_to_excel("D:/Desktop/INST.xlsx")


if __name__ == "__main__":

    go()
