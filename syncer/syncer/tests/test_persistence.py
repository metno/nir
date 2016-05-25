import unittest
import syncer.persistence
from datetime import datetime
import uuid


class IdentifiableObject(object):
    def __init__(self):
        self.id = str(uuid.uuid4())


class Product(IdentifiableObject):
    def __init__(self, id=None):
        if id:
            self.id = id
        else:
            self.id = str(uuid.uuid4())


class ProductInstance(IdentifiableObject):
    def __init__(self, reference_time, version, product_id=None):
        IdentifiableObject.__init__(self)
        self.product = Product(product_id)
        self.reference_time = reference_time
        self.version = version


class StateDatabaseTest(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.sb = syncer.persistence.StateDatabase(':memory:', create_if_missing=True)

    def test_is_loaded_false_on_empty_dataset(self):
        self.assertFalse(self.sb.is_loaded('foo'))

    def test_is_loaded_true_when_previously_marked(self):
        self.sb.set_loaded('a')
        self.assertTrue(self.sb.is_loaded('a'))
        self.assertFalse(self.sb.is_loaded('b'))

    def test_multiple_set_loaded_fails(self):
        # This would be a bug
        self.sb.set_loaded('a')
        try:
            self.sb.set_loaded('a')
            self.fail('should have thrown an exception')
        except:
            pass

    def test_pending_productinstances_empty_on_start(self):
        self.assertFalse(self.sb.pending_productinstances())

    def test_add_productinstance_to_be_processed(self):
        pi = ProductInstance(datetime.now(), 1)
        self.sb.add_productinstance_to_be_processed(pi)
        self.assertTrue(pi.id in self.sb.pending_productinstances())
        self.assertFalse(ProductInstance(datetime.now(), 2) in self.sb.pending_productinstances())

    def test_sort_productinstances_on_version(self):
        t = datetime.now()
        instances = [ProductInstance(t, 1, 'a'),
                     ProductInstance(t, 3, 'a'),
                     ProductInstance(t, 4, 'a'),
                     ProductInstance(t, 2, 'a')]
        for i in instances:
            self.sb.add_productinstance_to_be_processed(i)
        wanted_instance = instances[2].id
        self.assertTrue(wanted_instance in self.sb.pending_productinstances())

    def test_sort_productinstances_on_time(self):
        t1 = datetime(2016, 5, 10, 6)
        t2 = datetime(2016, 5, 10, 7)
        instances = [ProductInstance(t2, 1, 'a'),
                     ProductInstance(t1, 2, 'a'),
                     ProductInstance(t1, 4, 'a')]
        for i in instances:
            self.sb.add_productinstance_to_be_processed(i)

        wanted_instance = instances[0].id
        self.assertTrue(wanted_instance in self.sb.pending_productinstances())

    def test_separate_multiple_productinstances(self):
        t = datetime(2016, 5, 10, 6)
        instances = [ProductInstance(t, 1, 'a'),
                     ProductInstance(t, 1, 'b')]
        for i in instances:
            self.sb.add_productinstance_to_be_processed(i)

        for i in instances:
            self.assertTrue(i.id in self.sb.pending_productinstances())

    def test_done_removes_all_product_entries(self):
        t1 = datetime(2016, 5, 10, 6)
        t2 = datetime(2016, 5, 10, 7)
        instances = [ProductInstance(t2, 1, 'a'),
                     ProductInstance(t1, 2, 'a'),
                     ProductInstance(t1, 4, 'b')]
        for i in instances:
            self.sb.add_productinstance_to_be_processed(i)
        self.sb.done(instances[0])
        self.assertFalse(instances[0].id in self.sb.pending_productinstances())
        self.assertFalse(instances[1].id in self.sb.pending_productinstances())
        self.assertTrue(instances[2].id in self.sb.pending_productinstances())

    def test_select_latest_product_entry(self):
        product = Product()
        instances = [ProductInstance(datetime(2016, 5, 10, 6), 1, product.id),
                     ProductInstance(datetime(2016, 5, 10, 7), 1, product.id)]
        for i in instances:
            self.sb.add_productinstance_to_be_processed(i)
        self.assertFalse(instances[0].id in self.sb.pending_productinstances())
        self.assertTrue(instances[1].id in self.sb.pending_productinstances())
