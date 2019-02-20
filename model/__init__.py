# -*- coding:utf-8 -*-

from . import db
from . import table
from . import utils


db.init()

table.init()
table.User.register()
table.Customer.register()
table.DepositAccount.register()
table.DepositData.register()
table.DepositOwner.register()
table.create_all()
