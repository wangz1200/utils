# -*- coding:utf-8 -*-

from . import sa
from . import db
from . import table
from . import days


DEMAND = 1
FIX = 2
FIXES = {3: "03", 6: "06", 12: "12", 24: "24", 36: "36", 60: "60", }


class MetaDeposit(object):

    def __init__(self, date, scale=10000, precision=2):
        super(MetaDeposit, self).__init__()

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

    @property
    def select_deposit_by_user(self):
        return sa.select([
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

    @property
    def select_deposit_by_customer(self):
        return sa.select([
            self.customer.c.customer.label("customer"),
            self.customer.c.name.label("name"),
            self.customer.c.type.label("type"),
            self.customer.c.open_date.label("open_date"),
            self.balance.label("balance"),
            self.month_avg.label("month_avg"),
            self.season_avg.label("season_avg"),
            self.year_avg.label("year_avg"),
            self.user.c.user.label("user"),
            self.user.c.name.label("user_name"),
            self.user.c.dept.label("dept"),
            self.user.c.state.label("state"),
        ]). \
            select_from(self.source). \
            group_by(self.customer.c.customer, self.user.c.user)

    @property
    def select_deposit_by_account(self):
        return sa.select([
            self.account.c.account.label("account"),
            self.account.c.inst.label("inst"),
            self.account.c.product.label("product"),
            self.customer.c.customer.label("customer"),
            self.customer.c.name.label("name"),
            self.customer.c.type.label("type"),
            self.customer.c.open_date.label("open_date"),
            self.balance.label("balance"),
            self.year_avg.label("year_avg"),
            self.user.c.user.label("user"),
            self.user.c.name.label("user_name"),
            self.user.c.dept.label("state"),
            self.user.c.state.label("state"),
        ]). \
            select_from(self.source). \
            group_by(self.account.c.account, self.customer.c.customer, self.user.c.user)

    @property
    def select_deposit_by_inst(self):
        return sa.select([
            self.account.c.inst.label("inst"),
            self.balance.label("balance"),
            self.month_avg.label("month_avg"),
            self.season_avg.label("season_avg"),
            self.year_avg.label("year_avg"),
        ]).\
            select_from(self.source).\
            group_by(self.account.c.inst)


class Insert(object):

    def __init__(self, t):
        super(Insert, self).__init__()
        self.t = t
        self.sql = sa.insert(self.t)

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


class SelectDeposit(object):

    def __init__(self, date, scale=10000, precision=2):
        super(SelectDeposit, self).__init__()

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

    def by_user(self):
        return sa.select([
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

    def by_customer(self):
        return sa.select([
            self.customer.c.customer.label("customer"),
            self.customer.c.name.label("name"),
            self.customer.c.type.label("type"),
            self.customer.c.open_date.label("open_date"),
            self.balance.label("balance"),
            self.month_avg.label("month_avg"),
            self.season_avg.label("season_avg"),
            self.year_avg.label("year_avg"),
            self.user.c.user.label("user"),
            self.user.c.name.label("user_name"),
            self.user.c.dept.label("dept"),
            self.user.c.state.label("state"),
        ]). \
            select_from(self.source). \
            group_by(self.customer.c.customer, self.user.c.user)

    def by_account(self):
        return sa.select([
            self.account.c.account.label("account"),
            self.account.c.inst.label("inst"),
            self.account.c.product.label("product"),
            self.customer.c.customer.label("customer"),
            self.customer.c.name.label("name"),
            self.customer.c.type.label("type"),
            self.customer.c.open_date.label("open_date"),
            self.balance.label("balance"),
            self.year_avg.label("year_avg"),
            self.user.c.user.label("user"),
            self.user.c.name.label("user_name"),
            self.user.c.dept.label("state"),
            self.user.c.state.label("state"),
        ]). \
            select_from(self.source). \
            group_by(self.account.c.account, self.customer.c.customer, self.user.c.user)

    def by_inst(self):
        return sa.select([
            self.account.c.inst.label("inst"),
            self.balance.label("balance"),
            self.month_avg.label("month_avg"),
            self.season_avg.label("season_avg"),
            self.year_avg.label("year_avg"),
        ]).\
            select_from(self.source).\
            group_by(self.account.c.inst)
