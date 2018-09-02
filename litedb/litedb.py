import sqlite3
from contextlib import contextmanager
from recordclass import recordclass as rc

class Session:

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.con = sqlite3.connect(self.db)
        self.con.isolation_level = None
        #self.con.set_trace_callback(print)
        self.cursor = None
        return self

    def __exit__(self, *exc):
        if self.cursor:
            self.cursor.close()
        self.con.close()
        return None

    def begin(self):
        self.cursor = self.con.cursor()

    def end(self):
        self.cursor.close()

    def script(self, content):
        self.cursor.executescript(content)
    
    def sql(self, sql, param):
        if isinstance(param, Table) or 'insert ' not in sql:
            self.cursor.execute(sql, vars(param.entry))
        elif isinstance(param, list):
            pl = param
            if isinstance(param[0], Table):
                pl = [p.entry for p in param]
            self.cursor.executemany(sql, pl)

    def init_query(self, sql, param):
        self.cursor.execute(sql, param)

    def fetch(self):
        while True:
            r = self.cursor.fetchone()
            if not r:
                return
            yield r

    def fetch_all(self):
        return self.cursor.fetchall()

    def fetch_one(self):
        return self.cursor.fetchone()

    def commit(self):
        self.con.commit()


class MetaTable(type):
    def __new__(cls, clsname, supercls, attrd):
        if 'fields' not in attrd and clsname != "Table":
            raise TypeError("Table must have fields")
        elif clsname != "Table":
            # search primary key
            pkey = None
            for k, v in attrd['fields'].items():
                if ' primary key' in v:
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
                attrd['update_if'] = MetaTable.update_if(clsname)
                attrd['delete'] = MetaTable.delete(clsname, attrd['primary_key'])
                attrd['delete_if'] = MetaTable.delete_if(clsname)
                attrd['recordclass'] = rc(clsname, sorted(fields.keys()))
                print(repr(attrd['recordclass']))
        return type.__new__(cls, clsname, supercls, attrd)

    def insert(t, lsfield):
        return "insert into {table} ({fields}) values ({ph});".format(table=t, fields=", ".join(lsfield), ph=", ".join([':%s' % v for v in lsfield]))

    def update(t, lsfield, pkey):
        # extract primary key
        f = []
        for n in lsfield:
            if n != pkey:
                f.append(n)
        return "update {table} set {fields} where {condition};".format(
                table=t, fields=", ".join(["%s = :%s" % (v, v) for v in f]), condition="%s = :%s" % (pkey, pkey)
            )

    def update_if(t):
        return "update " + t + " set {fields} where {condition};"

    def delete(t, pkey):
        return "delete from {table} where {condition};".format(table=t, condition="%s = :%s" % (pkey, pkey))

    def delete_if(t):
        return "delete from " + t + " where {condition};"

class Table(metaclass=MetaTable):
    def __init__(self, *arg, **kwarg):
        t = type(self)
        if len(arg):
            self.entry = t.recordclass(*arg)
        elif len(kwarg):
            # add default (None) field
            if len(kwarg) != len(t.fields):
                for k in t.fields.keys():
                    if k not in kwarg:
                        kwarg[k] = None
            self.entry = t.recordclass(**kwarg)

    def __repr__(self):
        return repr(self.entry)
    
    @classmethod
    def do_insert(cls, s, param):
        s.sql(cls.insert, param)

    @classmethod
    def do_update(cls, s, param):
        s.sql(cls.update, param)
