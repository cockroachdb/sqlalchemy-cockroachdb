import unittest

from cockroach import errors
from cockroach.interface import TransactionOptions
from cockroach.kv import KV
from cockroach.methods import Methods
from cockroach.proto import api_pb2, errors_pb2
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

    # Verify the proper unwrapping and re-wrapping of the client's sender when
    # starting a transaction. Also verifies that user and user_priority
    # are propagated to the transactional client.
    def test_transaction_sender(self):
        client = KV(TestSender(lambda call: None), user="foo", user_priority=101)

        def callback(txn):
            self.assertIsNot(client, txn)
            self.assertIs(client.sender(), txn.sender())
            self.assertEqual(client.user, txn.user)
            self.assertEqual(client.user_priority, txn.user_priority)
        client.run_transaction(TransactionOptions(), callback)

    # Verify that trying to create nested transactions returns an error.
    def test_nested_transactions(self):
        client = KV(TestSender(lambda call: None))

        def callback(txn):
            with self.assertRaises(Exception):
                txn.run_transaction(TransactionOptions(), lambda kv: None)
        client.run_transaction(TransactionOptions(), callback)

    # Verify that transaction is committed upon successful invocation of the retryable func.
    def test_commit_transaction(self):
        count = [0]

        def handler(call):
            count[0] += 1
            self.assertIs(call.method, Methods.EndTransaction)
            self.assertTrue(call.args.commit)
        client = KV(TestSender(handler))
        client.run_transaction(TransactionOptions(), lambda kv: None)
        self.assertEqual(count[0], 1)

    # Verify that if the transaction is ended explicitly in the retryable func,
    # it is not automatically ended a second time at completion of retryable func.
    def test_commit_transaction_once(self):
        count = [0]

        def handler(call):
            count[0] += 1
        client = KV(TestSender(handler))

        def retryable(txn):
            txn.call(Methods.EndTransaction, api_pb2.EndTransactionRequest(commit=True))
        client.run_transaction(TransactionOptions(), retryable)
        self.assertEqual(count[0], 1)

    # Verify that transaction is aborted upon failed invocation of the retryable func.
    def test_abort_transaction(self):
        count = [0]

        def handler(call):
            count[0] += 1
            self.assertIs(call.method, Methods.EndTransaction)
            self.assertFalse(call.args.commit)
        client = KV(TestSender(handler))

        with self.assertRaises(Exception):
            client.run_transaction(TransactionOptions(), lambda txn: 1/0)
        self.assertEqual(count[0], 1)

    def test_retry_on_errors(self):
        # error proto, error class
        test_cases = [
            (errors_pb2.Error(detail=errors_pb2.ErrorDetail(read_within_uncertainty_interval=errors_pb2.ReadWithinUncertaintyIntervalError())), None),
            (errors_pb2.Error(detail=errors_pb2.ErrorDetail(transaction_aborted=errors_pb2.TransactionAbortedError())), None),
            (errors_pb2.Error(detail=errors_pb2.ErrorDetail(transaction_push=errors_pb2.TransactionPushError())), None),
            (errors_pb2.Error(), errors.GenericError),
            (errors_pb2.Error(detail=errors_pb2.ErrorDetail(range_not_found=errors_pb2.RangeNotFoundError())),
             errors.RangeNotFoundError),
            (errors_pb2.Error(detail=errors_pb2.ErrorDetail(range_key_mismatch=errors_pb2.RangeKeyMismatchError())),
             errors.RangeKeyMismatchError),
            (errors_pb2.Error(detail=errors_pb2.ErrorDetail(transaction_status=errors_pb2.TransactionStatusError())),
             errors.TransactionStatusError),
        ]
        for error_proto, error_class in test_cases:
            count = [0]

            def handler(call):
                if call.method is Methods.Put:
                    count[0] += 1
                    if count[0] == 1:
                        call.reply.header.error.CopyFrom(error_proto)
            client = KV(TestSender(handler))

            def callback(txn):
                txn.call(Methods.Put, test_put_request)
            try:
                client.run_transaction(TransactionOptions(), callback)
            except errors.ProtoError as e:
                proto_error = e
            else:
                proto_error = None

            if error_class is None:
                self.assertEqual(count[0], 2)
                self.assertIsNone(proto_error,
                                  "expected success on retry; got %s" % proto_error)
            else:
                self.assertEqual(count[0], 1)
                self.assertIsInstance(proto_error, error_class)
