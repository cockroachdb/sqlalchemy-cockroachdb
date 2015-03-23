from concurrent.futures import ThreadPoolExecutor
import logging
import os
import threading
import unittest

from cockroach import errors
from cockroach.interface import KVSender, TransactionOptions
from cockroach.kv import KV
from cockroach.http_sender import HTTPSender
from cockroach.methods import Methods
from cockroach.proto import api_pb2, data_pb2


class NotifyingSender(KVSender):
    """NotifyingSender wraps a KVSender to provide notifications when an RPC is sent."""
    def __init__(self, sender):
        self.wrapped = sender
        self.callback = None

    def reset(self, callback):
        """Schedules callback to be run after the next send.

        It is undefined to call reset while a previous callback is pending.
        """
        self.callback = callback

    def send(self, call):
        self.wrapped.send(call)
        if self.callback is not None:
            self.callback()
            self.callback = None

    def close(self):
        self.wrapped.close()


@unittest.skipIf('COCKROACH_PORT' not in os.environ, "not running under docker-compose")
class ClientTest(unittest.TestCase):
    def setUp(self):
        self.assertTrue(os.environ['COCKROACH_PORT'].startswith('tcp://'))
        addr = os.environ['COCKROACH_PORT'][len('tcp://'):]
        self.notifying_sender = NotifyingSender(HTTPSender(addr))
        self.client = KV(self.notifying_sender, user="root")
        self.executor = ThreadPoolExecutor(1)

    def tearDown(self):
        self.client.close()
        self.executor.shutdown()

    # Verify that we can make a simple RPC to the server.
    def test_basic(self):
        reply = self.client.call(
            Methods.Increment,
            api_pb2.IncrementRequest(
                header=api_pb2.RequestHeader(key=b"a"),
                increment=3,
            ))
        self.assertEqual(reply.new_value, 3)

    # Verify that non-transactional client will succeed despite write/write and read/write
    # conflicts. In the case where the non-transactional put can push the txn,
    # we expect the transaction's value to be written after all retries are complete.
    def test_retry_non_txn(self):
        # method, isolation, can push, expected attempts
        test_cases = [
            # Write/write conflicts.
            (Methods.Put, data_pb2.SNAPSHOT, True, 2),
            (Methods.Put, data_pb2.SERIALIZABLE, True, 2),
            # The write/write can't-push test cases take 15 seconds each.
            # TODO: why? because the go version uses Store.SetRangeRetryOptions?
            #(Methods.Put, data_pb2.SNAPSHOT, False, 1),
            #(Methods.Put, data_pb2.SERIALIZABLE, False, 1),
            # Read/write conflicts.
            (Methods.Get, data_pb2.SNAPSHOT, True, 1),
            (Methods.Get, data_pb2.SERIALIZABLE, True, 2),
            (Methods.Get, data_pb2.SNAPSHOT, False, 1),
            (Methods.Get, data_pb2.SERIALIZABLE, False, 1),
        ]

        # Lay down a write intent using a txn and attempt to write to same
        # key. Try this twice--once with priorities which will allow the
        # intent to be pushed and once with priorities which will not.
        for i, test_case in enumerate(test_cases):
            method, isolation, can_push, exp_attempts = test_case
            logging.info("starting test case %d", i)
            key = ("key-%d" % i).encode('ascii')
            txn_pri = -1
            client_pri = -1
            if can_push:
                client_pri = -2
            else:
                txn_pri = -2
            self.client.user_priority = client_pri
            done_call = threading.Event()
            count = [0]

            def callback(txn):
                txn.user_priority = txn_pri
                count[0] += 1
                # Lay down the intent.
                txn.call(Methods.Put, api_pb2.PutRequest(
                    header=api_pb2.RequestHeader(key=key),
                    value=data_pb2.Value(bytes=b"txn-value")))

                # On the first attempt, send the non-txn put or get.
                if count[0] == 1:
                    event = threading.Event()
                    # We use a "notifying" sender here, which allows us to know exactly
                    # when the call has been processed; otherwise, we'd be dependent on
                    # timing.
                    self.notifying_sender.reset(event.set)

                    def non_txn_op():
                        if method is Methods.Put:
                            args = api_pb2.PutRequest()
                            args.value.bytes = b"value"
                        elif method is Methods.Get:
                            args = api_pb2.GetRequest()
                        else:
                            raise Exception("unexpected method %s" % method)
                        args.header.key = key
                        while True:
                            try:
                                self.client.call(method, args)
                            except errors.WriteIntentError:
                                continue
                            except Exception:
                                # Run until we either succed or get a non-write-intent error.
                                pass
                            break
                        done_call.set()
                    self.executor.submit(non_txn_op)
                    event.wait()
            self.client.run_transaction(TransactionOptions(isolation=isolation), callback)

            # Make sure non-txn put or get has finished.
            done_call.wait()

            # Get the current value to verify whether the txn happened first.
            get_reply = self.client.call(
                Methods.Get, api_pb2.GetRequest(header=api_pb2.RequestHeader(key=key)))
            if can_push or method is Methods.Get:
                self.assertEqual(get_reply.value.bytes, b"txn-value")
            else:
                self.assertEqual(get_reply.value.bytes, b"value")
            self.assertEqual(count[0], exp_attempts)
