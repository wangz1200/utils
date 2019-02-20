# -*- coding:utf-8 -*-

import sqlalchemy as sa
from . import db


__metadata__ = None


def create_all():
    __metadata__.create_all(checkfirst=True)


class _Table(object):

    NAME = None
    COLUMNS = None

    def __new__(cls, name=None):
        return cls.get(name)

    def __init__(self, name=None):
        pass

    @classmethod
    def get(cls, name=None):
        return __metadata__.tables.get(name or cls.NAME, None)

    @classmethod
    def register(cls, name=None):
        return sa.Table(name or cls.NAME, __metadata__, *cls.COLUMNS)

    @classmethod
    def create(cls, name=None):
        t = cls.register(name=name)
        t.create(checkfirst=True)
        return t


class User(_Table):

    NAME = "user"
    COLUMNS = (
        sa.Column("user", sa.VARCHAR(32)),
        sa.Column("password", sa.VARCHAR(256)),
        sa.Column("name", sa.VARCHAR(32)),
        sa.Column("dept", sa.VARCHAR(32)),
        sa.Column("state", sa.VARCHAR(32)),
        sa.PrimaryKeyConstraint("user"),)

    def __init__(self, name=None):
        pass


class Customer(_Table):

    NAME = "customer"
    COLUMNS = (
        sa.Column("customer", sa.VARCHAR(32)),
        sa.Column("name", sa.VARCHAR(128)),
        sa.Column("type", sa.VARCHAR(32)),
        sa.Column("open_date", sa.DATE),
        sa.PrimaryKeyConstraint("customer"),)

    def __init__(self, name=None):
        pass


class DepositAccount(_Table):

    NAME = "deposit_account"
    COLUMNS = (
        sa.Column("account", sa.VARCHAR(32)),
        sa.Column("customer", sa.VARCHAR(32), ),
        sa.Column("inst", sa.VARCHAR(32)),
        sa.Column("product", sa.VARCHAR(32)),
        sa.Column("open_date", sa.DATE),
        sa.PrimaryKeyConstraint("account"),
        sa.ForeignKeyConstraint(("customer", ), ["customer.customer", ]),)

    def __init__(self, name=None):
        pass


class DepositData(_Table):

    NAME = "deposit_data"
    COLUMNS = (
        sa.Column("account", sa.VARCHAR(32)),
        sa.Column("state", sa.VARCHAR(32)),
        sa.Column("balance", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("month_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("season_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("year_acc", sa.NUMERIC(32, 2, asdecimal=False)),
        sa.Column("date", sa.DATE),
        sa.ForeignKeyConstraint(("account", ), ["deposit_account.account", ]),)

    def __init__(self, name=None):
        pass

    @classmethod
    def from_date(cls, date):
        t = cls()
        return sa.select([t, ]).where(t.c.date == date).alias("data")


class DepositOwner(_Table):

    NAME = "deposit_owner"
    COLUMNS = (
        sa.Column("customer", sa.VARCHAR(32)),
        sa.Column("user", sa.VARCHAR(32)),
        sa.PrimaryKeyConstraint("customer"),
        sa.ForeignKeyConstraint(("user", ), ["user.user", ]),
        sa.ForeignKeyConstraint(("customer", ), ["customer.customer", ]),)

    def __init__(self, name=None):
        pass


def init():
    global __metadata__

    if db.__engine__ is None:
        raise Exception("db.engine is None")

    __metadata__ = sa.MetaData(bind=db.__engine__)

