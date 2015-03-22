import unittest

from cockroach.kv import KV
from cockroach.methods import Methods
from cockroach.proto import api_pb2
from cockroach.test_http_sender import test_put_request
from cockroach.test_txn_sender import TestSender


class KVTest(unittest.TestCase):
    # Verify that flushing without preparing any calls is a noop.
    def test_empty_flush(self):
        count = [0]

        def handler(call):
            count[0] += 1
        client = KV(TestSender(handler))
        client.flush()
        self.assertEqual(count[0], 0)

    # Verify that client command id is set on call.
    def test_client_command_id(self):
        count = [0]

        def handler(call):
            count[0] += 1
            self.assertNotEqual(call.args.header.cmd_id.wall_time, 0,
                                "expected client command id to be initialized")
        client = KV(TestSender(handler))
        client.call(Methods.Put, test_put_request)
        self.assertEqual(count[0], 1)

    # Verify that flush sends single prepared call without a batch and more
    # than one prepared call with a batch.
    def test_prepare_and_flush(self):
        for i in range(1, 3):
            count = [0]

            def handler(call):
                count[0] += 1
                if i == 1:
                    self.assertIsNot(call.method, Methods.Batch,
                                     "expected non-batch for a single buffered call")
                elif i > 1:
                    self.assertIs(call.method, Methods.Batch,
                                  "expected batch for >1 buffered calls")
                    self.assertNotEqual(call.args.header.cmd_id.wall_time, 0,
                                        "expected batch client command id to be initialized")
            client = KV(TestSender(handler))

            for j in range(i):
                client.prepare(Methods.Put, test_put_request, api_pb2.PutResponse())
            client.flush()
            self.assertEqual(count[0], 1)

    # Verify that call will act as a prepare followed by a flush if there are
    # already prepared and unflushed calls buffered.
    def test_prepare_and_call(self):
        for i in range(0, 3):
            count = [0]

            def handler(call):
                count[0] += 1
                if i == 0:
                    self.assertIsNot(call.method, Methods.Batch,
                                     "expected non-batch for a single buffered call")
                elif i > 0:
                    self.assertIs(call.method, Methods.Batch,
                                  "expected batch for >1 buffered calls")
                    self.assertNotEqual(call.args.header.cmd_id.wall_time, 0,
                                        "expected batch client command id to be initialized")
                    self.assertEqual(len(call.args.requests), i+1)
            client = KV(TestSender(handler))

            for j in range(i):
                client.prepare(Methods.Put, test_put_request, api_pb2.PutResponse())
            reply = client.call(Methods.Put, test_put_request)
            self.assertEqual(count[0], 1)
            self.assertIsNotNone(reply)
