import unittest
import uuid

from cockroach.call import Call
from cockroach.interface import KVSender
from cockroach.methods import Methods
from cockroach.proto import data_pb2, errors_pb2
from cockroach.txn_sender import TxnSender, TransactionOptions
from cockroach.test_http_sender import test_put_request, test_put_response

txn_key = b"test-txn"
txn_id = str(uuid.uuid4()).encode('ascii')


def make_ts(wall_time, logical):
    return data_pb2.Timestamp(wall_time=wall_time, logical=logical)


def timestamp_equal(a, b):
    return a.wall_time == b.wall_time and a.logical == b.logical


class TestSender(KVSender):
    def __init__(self, handler):
        self.handler = handler

    def send(self, call):
        header = call.args.header
        header.user_priority = -1
        if header.HasField('txn') and not header.txn.id:
            header.txn.key = txn_key
            header.txn.id = txn_id
        call.reply.Clear()
        if call.method is Methods.Put:
            call.reply.MergeFrom(test_put_response)
        call.reply.header.txn.CopyFrom(header.txn)

        if self.handler is not None:
            self.handler(call)


class TxnSenderTest(unittest.TestCase):
    # Verify that response txn timestamp is always upgraded on successive requests.
    def test_txn_timestamp(self):
        # expected request timestamp, response timestamp
        test_cases = [
            (make_ts(0, 0), make_ts(10, 0)),
            (make_ts(10, 0), make_ts(10, 1)),
            (make_ts(10, 1), make_ts(10, 0)),
            (make_ts(10, 1), make_ts(20, 1)),
            (make_ts(20, 1), make_ts(20, 1)),
            (make_ts(20, 1), make_ts(0, 0)),
            (make_ts(20, 1), make_ts(20, 1)),
            ]

        test_idx = [0]

        def handler(call):
            self.assertTrue(timestamp_equal(test_cases[test_idx[0]][0],
                                            call.args.header.txn.timestamp))
            call.reply.header.txn.timestamp.CopyFrom(test_cases[test_idx[0]][1])
        ts = TxnSender(TestSender(handler), TransactionOptions())

        for i, test_case in enumerate(test_cases):
            test_idx[0] = i
            ts.send(Call(Methods.Put, test_put_request))

    # Verify transaction is reset on abort.
    def test_reset_txn_on_abort(self):
        def handler(call):
            call.reply.header.txn.CopyFrom(call.args.header.txn)
            # This appears to be the least-clumsy way of setting the 'has'
            # bit without setting any field.
            call.reply.header.error.transaction_aborted.CopyFrom(
                errors_pb2.TransactionAbortedError())
        ts = TxnSender(TestSender(handler), TransactionOptions())

        ts.send(Call(Methods.Put, test_put_request))
        self.assertFalse(ts.txn.id, 'expected txn to be cleared')
