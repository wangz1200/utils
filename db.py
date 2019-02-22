# -*- coding:utf8 -*-


import sqlalchemy as sa


class Table(object):

    def __init__(self, engine):
        super(Table, self).__init__()
        self.metadata = sa.MetaData(bind=engine)

    def __call__(self, name):
        return self.metadata.tables.get(name, None)

    def register(self, name, *columns):
        return sa.Table(name, self.metadata, *columns)

    def create(self, name, *columns):
        t = self.register(name, *columns)
        t.create(checkfirst=True)
        return t

    def create_all(self):
        self.metadata.create_all(checkfirst=True)


class DB(object):

    __slots__ = "engine", "table"

    def __init__(
            self,
            host="127.0.0.1", port=5432, name="db",
            user="postgres", password="postgres",
            pool_size=8, recycle=3600, encoding="utf8"):
        
        super(DB, self).__init__()

        url = "postgresql://{user}:{password}@{host}:{port}/{name}".format(
            host=host, port=port, name=name, user=user, password=password)

        self.engine = sa.create_engine(url, pool_size=pool_size, pool_recycle=recycle, encoding=encoding)
        self.table = Table(self.engine)

    @property
    def t(self):
        return self.table

    def ping(self):
        conn = None
        try:
            conn = self.engine.connect()
            conn.ping()
            return True
        except Exception:
            return False
        finally:
            if conn is not None:
                conn.close()

    def query(self, sql):
        conn = None
        try:
            conn = self.engine.connect()
            res = conn.execute(sql).fetchall()
        except Exception as err:
            res = err
        finally:
            if conn is not None:
                conn.close()
        return res

    def commit(self, sql, *args, **kw):
        conn = self.engine.connect()
        trans = None
        try:
            trans = conn.begin()
            conn.execute(sql, *args, **kw)
            trans.commit()
            ret = "OK"
        except Exception as err:
            if trans is not None:
                trans.rollback()
            ret = err
        finally:
            if conn:
                conn.close()
        return ret

