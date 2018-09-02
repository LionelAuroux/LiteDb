import unittest

import litedb as db

class Entity(db.Table):
    """
    For test
    """
    fields = {
            'ent_id': "integer primary key autoincrement",
            'ent_type': "integer not null"
        }

class Desc(db.Table):
    fields = {
        'desc_id': 'integer primary key autoincrement',
        'desc_name': 'varchar(20) not null',
        'desc_long': 'text'
        }

class LiteDB_Test(unittest.TestCase):
    def test_00(self):
        """
        Formatting test
        """
        # create
        self.assertTrue(hasattr(Entity, 'create'))
        cr = ("create table Entity (\n" +
            "ent_id integer primary key autoincrement,\n" +
            "ent_type integer not null\n)")
        self.assertEqual(Entity.create, cr, "Entity.create not formatted correctly")
        # reset
        self.assertTrue(hasattr(Entity, 'reset'))
        rs = "drop table if exists Entity;\n" + cr
        self.assertEqual(Entity.reset, rs, "Entity.reset not formatted correctly")
        # insert
        self.assertTrue(hasattr(Entity, 'insert'))
        self.assertEqual(Entity.insert, "insert into Entity (ent_id, ent_type) values (:ent_id, :ent_type);", "Entity.insert not formatted correctly")
        # update
        self.assertTrue(hasattr(Entity, 'update'))
        self.assertEqual(Entity.update, "update Entity set ent_type = :ent_type where ent_id = :ent_id;", "Entity.update not formatted correctly")
        # delete
        self.assertTrue(hasattr(Entity, 'delete'))
        self.assertEqual(Entity.delete, "delete from Entity where ent_id = :ent_id;", "Entity.delete not formatted correctly")

    def test_01(self):
        import os
        # reset if any
        if os.path.exists("Model.db"):
            os.remove("Model.db")
        # just test a session
        t1 = False
        with db.Session("Model.db") as s:
            if os.path.exists("Model.db"):
                t1 = True
                s.begin()
                s.script(Desc.reset)
                s.end()
        self.assertTrue(t1)
        # insert some data
        with db.Session("Model.db") as s:
            s.begin()
            Desc.do_insert(s, [Desc(desc_name="test", desc_long="this is a test"),
                    Desc(desc_name="test2", desc_long="this is a test2"),
                    Desc(desc_name="test3", desc_long="this is a test3")])
            s.end()
            s.begin()
            Desc.do_insert(s, Desc(desc_name="test4", desc_long="this is a test4"))
            s.end()
        # fetch
        with db.Session("Model.db") as s:
            s.begin()
            s.init_query("select desc_id from Desc where desc_long like ?", ["this%"])
            self.assertEqual(len(list(s.fetch())), 4, "count of inserted rows are wrong")
            s.end()
        # update
        with db.Session("Model.db") as s:
            s.begin()
            s.init_query("select * from Desc where desc_name=?", ["test2"])
            row = Desc(*s.fetch_one())
            s.end()
            theid = row.entry.desc_id
            row.entry.desc_name = 'test_new'
            s.begin()
            Desc.do_update(s, row)
            s.end()
            s.begin()
            s.init_query("select * from Desc where desc_id=?", [theid])
            row = Desc(*s.fetch_one())
            s.end()
            self.assertEqual(row.entry.desc_name, 'test_new', "update failed")
