import unittest

import litedb as db

class Entity(db.Table):
    """
    Individual, or group, or kind/population
    """
    fields = {
            'ent_id': "integer primary key autoincrement",
            'ent_type': "integer not null"
        }

class LiteDB_Test(unittest.TestCase):
    def test_00(self):
        self.assertTrue(hasattr(Entity, 'create'))
        print(Entity.create)
        self.assertTrue(hasattr(Entity, 'insert'))
        print(Entity.insert)
        self.assertTrue(hasattr(Entity, 'update'))
        print(Entity.update)
        self.assertTrue(hasattr(Entity, 'reset'))
        print(Entity.reset)
