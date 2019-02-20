# -*- coding:utf8 -*-


import sqlalchemy as sa


class DB(object):

    __slots__ = "engine", "metadata"

    def __init__(
            self,
            host="127.0.0.1", port=5432, name="ims",
            user="postgres", password="postgres",
            pool_size=8, recycle=3600, encoding="utf8"):
        super(DB, self).__init__()

        url = "postgresql://{user}:{password}@{host}:{port}/{name}".format(
            host=host, port=port, name=name, user=user, password=password)

        self.engine = sa.create_engine(url, pool_size=pool_size, pool_recycle=recycle, encoding=encoding)
        self.metadata = sa.MetaData(bind=self.engine)

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

    def table(self, name):
        return self.metadata.tables.get(name, None)

    def register_table(self, name, *columns):
        return sa.Table(name, self.metadata, *columns)

    def create_table(self, name, *columns):
        t = self.register_table(name, *columns)
        t.create(checkfirst=True)
        return t

    def create_all_tables(self):
        self.metadata.create_all(checkfirst=True)

