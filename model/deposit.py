# -*- coding: utf-8 -*-

import sqlalchemy as sa
from . import


def register_user_table(db, name="user"):
    return db.register_table(
        name,
        sa.Column("user", sa.VARCHAR(32)),
        sa.Column("password", sa.VARCHAR(256)),
        sa.Column("name", sa.VARCHAR(32)),
        sa.Column("dept", sa.VARCHAR(32)),
        sa.Column("state", sa.VARCHAR(32)),
        sa.PrimaryKeyConstraint("user"),
    )


def register_customer_table(db, name="customer"):
    return db.register_table(
        name,
        sa.Column("customer", sa.VARCHAR(32)),
        sa.Column("name", sa.VARCHAR(128)),
        sa.Column("type", sa.VARCHAR(32)),
        sa.Column("open_date", sa.DATE),
        sa.PrimaryKeyConstraint("customer"),
    )


def register_deposit_account_table(db, name="deposit_account"):
    return db.register_table(
        name,
        sa.Column("account", sa.VARCHAR(32)),
        sa.Column("customer", sa.VARCHAR(32), ),
        sa.Column("inst", sa.VARCHAR(32)),
        sa.Column("product", sa.VARCHAR(32)),
        sa.Column("open_date", sa.DATE),
        sa.PrimaryKeyConstraint("account"),
        sa.ForeignKeyConstraint(("customer",), ["customer.customer", ]),
    )


def register_deposit_data_table(db, name="deposit_data"):
    return db.register_table(
        name,
        sa.Column("account", sa.VARCHAR(32)),
        sa.Column("state", sa.VARCHAR(32)),
        sa.Column("balance", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("month_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("season_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("year_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("date", sa.DATE),
        sa.ForeignKeyConstraint(("account",), ["deposit_account.account", ]),
    )


def register_deposit_owner(db, name="deposit_owner"):
    return db.register_table(
        name,
        sa.Column("customer", sa.VARCHAR(32)),
        sa.Column("user", sa.VARCHAR(32)),
        sa.PrimaryKeyConstraint("customer"),
        sa.ForeignKeyConstraint(("user",), ["user.user", ]),
        sa.ForeignKeyConstraint(("customer",), ["customer.customer", ]),
    )


class MetaDeposit(object):

    def __init__(self, db, date, scale=10000, precision=2):
        super(MetaDeposit, self).__init__()

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

        m, s, y = days.days(date)
        self.balance = sa.func.round(sa.func.sum(self.data.c.balance) / scale, precision)
        self.month_avg = sa.func.round(sa.func.sum(self.data.c.month_acc) / scale / m, precision)
        self.season_avg = sa.func.round(sa.func.sum(self.data.c.season_acc) / scale / s, precision)
        self.year_avg = sa.func.round(sa.func.sum(self.data.c.year_acc) / scale / y, precision)