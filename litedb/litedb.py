import sqlite3
import re
from contextlib import contextmanager
from recordclass import recordclass as rc

class Session:

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.con = sqlite3.connect(self.db)
        self.con.isolation_level = None
        self.cursor = None
        return self

    def __exit__(self, *exc):
        if self.cursor:
            self.cursor.close()
        self.con.close()
        return None

    def open(self):
        self.__enter__()

    def close(self):
        self.__exit__()
    
    def trace_on(self):
        self.con.set_trace_callback(print)

    def trace_off(self):
        self.con.set_trace_callback(None)

    def begin(self):
        print("TRANSACTION %s" % self.con.in_transaction)
        self.con.isolation_level = ''
        print("TRANSACTION %s" % self.con.in_transaction)

    def end(self):
        print("TRANSACTION %s" % self.con.in_transaction)
        self.con.isolation_level = None
        print("TRANSACTION %s" % self.con.in_transaction)

    def script(self, content):
        self.con.executescript(content)
    
    def sql(self, sql, param):
        if isinstance(param, Table) or 'insert ' not in sql:
            if type(param) is dict:
                self.con.execute(sql, param)
            else:
                self.con.execute(sql, vars(param))
        elif isinstance(param, list):
            pl = param
            if isinstance(param[0], Table):
                pl = [p.entry for p in param]
            self.con.executemany(sql, pl)

    def init_query(self, sql, param=None):
        if self.cursor:
            self.cursor.close()
        self.cursor = self.con.cursor()
        if param is None:
            param = ()
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

    def rollback(self):
        self.con.rollback()

class MetaTable(type):
    def __new__(cls, clsname, supercls, attrd):
        # TODO add 'constraints' to add list of extra line during create table
        if 'fields' not in attrd and clsname != "Table":
            raise TypeError("Table must have fields")
        elif clsname != "Table":
            # search for simple primary key
            lspkey = None
            for k, v in attrd['fields'].items():
                if ' primary key' in v:
                    if lspkey:
                        raise TypeError("Table must have only one single primary key")
                    lspkey = [k]
            # handle composed primary key
            if 'constraints' in attrd:
                for c in attrd['constraints']:
                    if 'primary key' in c:
                        r = re.compile(r"^[^(]*\((\s*\S+\s*(?:,\s*\S+\s*)*)\).*$")
                        lstxt = r.fullmatch(c).groups()[0]
                        lspkey = list(map(str.strip, lstxt.split(',')))
            if not lspkey:
                raise TypeError("Table must have primary key")
            attrd['primary_key'] = lspkey
        fields = None
        if 'fields' in attrd:
            fields = attrd['fields']
        if 'init' not in attrd:
            attrd['init'] = True
            create = "create table if not exists %s (\n" % clsname
            if fields:
                lines = []
                for idx, k in enumerate(sorted(fields.keys())):
                    v = fields[k]
                    lines.append("%s %s" % (k, v))
                if 'constraints' in attrd:
                    lines.extend(attrd['constraints'])
                create += ",\n".join(lines)
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
        return type.__new__(cls, clsname, supercls, attrd)

    def insert(t, lsfield):
        return "insert into {table} ({fields}) values ({ph});".format(table=t, fields=", ".join(lsfield), ph=", ".join([':%s' % v for v in lsfield]))

    def update(t, lsfield, pkey):
        # extract primary key
        f = []
        for n in lsfield:
            if n not in pkey:
                f.append(n)
        conds = []
        for pk in pkey:
            conds.append("%s = :%s" % (pk, pk))
        return "update {table} set {fields} where {condition};".format(
                table=t, fields=", ".join(["%s = :%s" % (v, v) for v in f]), condition=" and ".join(conds)
            )

    def update_if(t):
        return "update " + t + " set {fields} where {condition};"

    def delete(t, pkey):
        conds = []
        for pk in pkey:
            conds.append("%s = :%s" % (pk, pk))
        return "delete from {table} where {condition};".format(table=t, condition=" and ".join(conds))

    def delete_if(t):
        return "delete from " + t + " where {condition};"

class Table(dict, metaclass=MetaTable):
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

    def __getitem__(self, k):
        return self.entry[k]

    @property
    def __dict__(self):
        return vars(self.entry)
    
    @classmethod
    def do_insert(cls, s, param):
        s.sql(cls.insert, param)

    @classmethod
    def do_update(cls, s, param):
        s.sql(cls.update, param)

    @classmethod
    def do_update_if(cls, s, **param):
        s.sql(cls.update_if.format(**param), {})

    @classmethod
    def do_delete(cls, s, theid):
        s.sql(cls.delete, theid)

    @classmethod
    def do_delete_if(cls, s, **param):
        s.sql(cls.delete_if.format(**param), {})
