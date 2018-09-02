import sqlite3
from contextlib import contextmanager

class Session:

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.con = sqlite3.connect(self.db)
        return self

    def __exit__(self, *exc):
        self.con.close()
        return None

    def script(self, content):
        self.con.executescript(content)
    
    def sql(self, sql, param):
        if isinstance(param, Table):
            self.con.execute(sql, param.entry)
        else:
            self.con.execute(sql, param)

    def init_query(self, sql, param):
        self.cursor = self.con.cursor()
        self.cursor.execute(sql, param)

    def fetch_res(self):
        yield self.cursor.fetchone()

    def fetch_all(self):
        return self.cursor.fetchall()

    def commit(self):
        self.con.commit()

from collections import namedtuple as nt

class MetaTable(type):
    def __new__(cls, clsname, supercls, attrd):
        if 'fields' not in attrd and clsname != "Table":
            raise TypeError("Table must have fields")
        elif clsname != "Table":
            # search primary key
            pkey = None
            for k, v in attrd['fields'].items():
                if ' primary key ' in v:
                    if pkey:
                        raise TypeError("Table must have only one primary key")
                    pkey = k
            if not pkey:
                raise TypeError("Table must have primary key")
            attrd['primary_key'] = pkey
        fields = None
        if 'fields' in attrd:
            fields = attrd['fields']
        if 'init' not in attrd:
            attrd['init'] = True
            create = "create table %s (\n" % clsname
            if fields:
                for idx, k in enumerate(sorted(fields.keys())):
                    v = fields[k]
                    create += "%s %s" % (k, v)
                    if idx < len(fields) - 1:
                        create += ",\n"
            create += "\n)"
            attrd['create'] = create
            attrd['reset'] = (
                "drop table if exists {0};\n" + create
                ).format(clsname)
            if fields:
                attrd['insert'] = MetaTable.insert(clsname, sorted(fields.keys()))
                attrd['update'] = MetaTable.update(clsname, sorted(fields.keys()), attrd['primary_key'])
                attrd['namedtuple'] = nt(clsname, sorted(fields.keys()))
        return type.__new__(cls, clsname, supercls, attrd)

    def insert(t, lsfield):
        return "insert into {table} ({fields}) values ({ph});".format(table=t, fields=", ".join(lsfield), ph=",".join(['?'] * len(lsfield)))

    def update(t, lsfield, pkey):
        # extract primary key
        f = []
        for n in lsfield:
            if n != pkey:
                f.append(n)
        return "update {table} set {fields} where {condition};".format(
                table=t, fields=", ".join([v + " = ?" for v in f]), condition="%s = ?" % pkey
            )

class Table(metaclass=MetaTable):
    def __init__(self, *arg):
        t = type(self)
        self.entry = t.namedtuple(*arg)

    def __repr__(self):
        return repr(self.entry)
