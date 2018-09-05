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

class Stats(db.Table):
    fields = {
        'stats_id': 'integer primary key autoincrement',
        'stats_val': 'integer not null',
        'stats_num': 'integer not null',
        }

class BiKey(db.Table):
    fields = {
        'bikey_txt': 'text not null',
        'bikey_num': 'integer not null',
        'bikey_num2': 'integer not null',
        'bikey_val': 'integer not null',
    }
    constraints = ['constraint "BiKey have a primary key" primary key(bikey_txt, bikey_num, bikey_num2)']

class LiteDB_Test(unittest.TestCase):
    def test_00(self):
        """
        Formatting test
        """
        # create
        self.assertTrue(hasattr(Entity, 'create'))
        cr = ("create table if not exists Entity (\n" +
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
        # bikey
        self.assertTrue(hasattr(BiKey, 'update'))
        self.assertEqual(BiKey.update, "update BiKey set bikey_val = :bikey_val where bikey_txt = :bikey_txt and bikey_num = :bikey_num and bikey_num2 = :bikey_num2;", "BiKey.update not formatted correctly")
        self.assertTrue(hasattr(BiKey, 'delete'))
        self.assertEqual(BiKey.delete, "delete from BiKey where bikey_txt = :bikey_txt and bikey_num = :bikey_num and bikey_num2 = :bikey_num2;", "BiKey.delete not formatted correctly")

    def test_01(self):
        """
        Basic features
        """
        import os
        # reset if any
        if os.path.exists("Model.db"):
            os.remove("Model.db")
        # just test a session
        t1 = False
        with db.Session("Model.db") as s:
            if os.path.exists("Model.db"):
                t1 = True
                s.script(Desc.reset)
        self.assertTrue(t1)
        # insert some data
        with db.Session("Model.db") as s:
            Desc.do_insert(s, [Desc(desc_name="test", desc_long="this is a test"),
                    Desc(desc_name="test2", desc_long="this is a test2"),
                    Desc(desc_name="test3", desc_long="this is a test3")])
            Desc.do_insert(s, Desc(desc_name="test4", desc_long="this is a test4"))
        # fetch
        with db.Session("Model.db") as s:
            s.init_query("select desc_id from Desc where desc_long like ?", ["this%"])
            self.assertEqual(len(list(s.fetch())), 4, "count of inserted rows are wrong")
        theid = None
        # update
        with db.Session("Model.db") as s:
            s.init_query("select * from Desc where desc_name=?", ["test2"])
            row = Desc(*s.fetch_one())
            theid = row.entry.desc_id
            row.entry.desc_name = 'test_new'
            Desc.do_update(s, row)
            s.init_query("select * from Desc where desc_id=?", [theid])
            row = Desc(*s.fetch_one())
            self.assertEqual(row.entry.desc_name, 'test_new', "update failed")
        # delete
        with db.Session("Model.db") as s:
            Desc.do_delete(s, {"desc_id": theid})
            s.init_query("select * from Desc where desc_name=?", ["test_new"])
            self.assertIs(s.fetch_one(), None, "row is not deleted")
        # delete_if
        with db.Session("Model.db") as s:
            s.script(Stats.reset)
            Stats.do_insert(s, [
                Stats(stats_val=1, stats_num=22),
                Stats(stats_val=2, stats_num=12),
                Stats(stats_val=3, stats_num=20),
                Stats(stats_val=4, stats_num=5),
                Stats(stats_val=5, stats_num=30),
                Stats(stats_val=6, stats_num=42),
            ])
            s.init_query("select stats_id from Stats")
            self.assertEqual(len(s.fetch_all()), 6, "count of inserted rows are wrong")
            Stats.do_delete_if(s, condition="stats_num > 20")
            s.init_query("select stats_id from Stats")
            self.assertEqual(len(s.fetch_all()), 3, "count of inserted rows are wrong")
        # update_if
        with db.Session("Model.db") as s:
            Stats.do_update_if(s, fields="stats_num = stats_num + 1", condition="stats_num < 10")
            s.init_query("select sum(stats_num) as S from Stats")
            r = s.fetch_one()[0]
            self.assertEqual(r, 38, "sum is'nt correct")

    def test_02(self):
        """
        Advanced features
        """
        # insert on BiKey
        with db.Session("Model.db") as s:
            s.script(BiKey.reset)
            BiKey.do_insert(s, [
                BiKey(bikey_txt='K1', bikey_num=1, bikey_num2=1, bikey_val=12),
                BiKey(bikey_txt='K1', bikey_num=1, bikey_num2=2, bikey_val=28),
                BiKey(bikey_txt='K1', bikey_num=2, bikey_num2=1, bikey_val=31),
                BiKey(bikey_txt='K2', bikey_num=1, bikey_num2=1, bikey_val=49),
                BiKey(bikey_txt='K2', bikey_num=1, bikey_num2=2, bikey_val=53),
            ])
            s.init_query("select * from BiKey;")
            self.assertEqual(len(list(s.fetch())), 5, "count of inserted rows are wrong")
        # delete
        with db.Session("Model.db") as s:
            BiKey.do_delete(s, {"bikey_txt": 'K1', 'bikey_num': 2, 'bikey_num2':1})
            s.init_query("select * from BiKey where bikey_num=2")
            self.assertIs(s.fetch_one(), None, "row is not deleted")
