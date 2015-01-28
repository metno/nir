"""
Tests for the ZeroMQ implementation.
"""

import zmq

import modelstatus.tests
import modelstatus.orm
import modelstatus.zeromq
import modelstatus.tests.test_utils


class TestZmq(modelstatus.tests.test_utils.TestBase):

    def before(self):
        self.orm = modelstatus.orm.get_sqlite_memory_session()
        self.setup_database_fixture()
        self.setup_zmq()

    def test_zmq_init(self):
        """
        Test that an instantiated ZeroMQ publisher class has a context object
        and a socket which listens to a specific.
        """
        self.assertIsInstance(self.zmq.context, zmq.Context)
        self.assertIsInstance(self.zmq.sock, zmq.Socket)

    def test_message_from_resource(self):
        object_ = self.orm.query(modelstatus.orm.ModelRun).get(1)
        zmq_msg = self.zmq.message_from_resource(object_)
        target_msg = 'model_run 1'
        self.assertEqual(zmq_msg, target_msg)
