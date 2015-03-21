import unittest

from cockroach.call import Call
from cockroach.methods import Methods
from cockroach.proto import api_pb2


class CallTest(unittest.TestCase):
    def test_reset_client_cmd_id(self):
        call = Call(Methods.Increment, api_pb2.IncrementRequest())
        call.reset_client_cmd_id()
        self.assertNotEqual(call.args.header.cmd_id.wall_time, 0)
        self.assertNotEqual(call.args.header.cmd_id.random, 0)
