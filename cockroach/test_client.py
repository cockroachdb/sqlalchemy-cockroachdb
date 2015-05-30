import logging
import os
import threading
import unittest

from cockroach import errors
from cockroach.interface import KVSender, TransactionOptions
from cockroach.kv import KV
from cockroach.http_sender import HTTPSender
from cockroach.methods import Methods
from cockroach.proto import api_pb2, config_pb2, data_pb2


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
        # This import is delayed because the module is not present by default
        # on python 2. We only run this test with python 3, but it needs to be
        # importable on py2.
        from concurrent.futures import ThreadPoolExecutor
        self.executor = ThreadPoolExecutor(2)

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
            # Some of the can't-push test cases take 15 seconds each,
            # and are currently failing when uncommented.
            # TODO: why? because the go version uses Store.SetRangeRetryOptions?
            #(Methods.Put, data_pb2.SNAPSHOT, False, 1),
            #(Methods.Put, data_pb2.SERIALIZABLE, False, 1),
            # Read/write conflicts.
            (Methods.Get, data_pb2.SNAPSHOT, True, 1),
            (Methods.Get, data_pb2.SERIALIZABLE, True, 2),
            (Methods.Get, data_pb2.SNAPSHOT, False, 1),
            #(Methods.Get, data_pb2.SERIALIZABLE, False, 1),
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

    def test_run_transaction(self):
        for commit in [True, False]:
            value = b"value"
            key = ("key-%s" % commit).encode("ascii")

            def callback(txn):
                # Put transactional value.
                txn.call(Methods.Put, api_pb2.PutRequest(header=api_pb2.RequestHeader(key=key),
                                                         value=data_pb2.Value(bytes=value)))
                # Attempt to read outside of txn.
                gr = self.client.call(
                    Methods.Get, api_pb2.GetRequest(header=api_pb2.RequestHeader(key=key)))
                self.assertFalse(gr.HasField('value'))
                # Read within the transaction.
                gr = txn.call(
                    Methods.Get, api_pb2.GetRequest(header=api_pb2.RequestHeader(key=key)))
                self.assertEqual(gr.value.bytes, value)
                if not commit:
                    raise ValueError("purposefully failing transaction")
            # Use snapshot isolation so non-transactional read can always push.
            try:
                self.client.run_transaction(
                    TransactionOptions(isolation=data_pb2.SNAPSHOT), callback)
            except ValueError as e:
                self.assertTrue((not commit) and str(e) == "purposefully failing transaction")
            else:
                self.assertTrue(commit)

            # Verify the value is now visible on commit==True, and not visible otherwise.
            gr = self.client.call(
                Methods.Get, api_pb2.GetRequest(header=api_pb2.RequestHeader(key=key)))
            if commit:
                self.assertEqual(gr.value.bytes, value)
            else:
                self.assertFalse(gr.HasField('value'))

    # Verify gets and puts of protobufs using the kv client's convenience methods.
    def test_get_and_put_proto(self):
        zone_config = config_pb2.ZoneConfig(
            replica_attrs=[config_pb2.Attributes(attrs=["dc1", "mem"]),
                           config_pb2.Attributes(attrs=["dc2", "mem"])],
            range_min_bytes=1<<10,  # 1k
            range_max_bytes=1<<18,  # 256k
        )
        key = b"zone-config"

        self.client.put_proto(key, zone_config)

        read_zone_config = config_pb2.ZoneConfig()
        ok, ts = self.client.get_proto(key, read_zone_config)
        self.assertTrue(ok)
        self.assertNotEqual(ts.wall_time, 0)
        self.assertEqual(read_zone_config, zone_config)

    # Verify that empty values are preserved for both empty bytes and integer=0.
    def test_empty_values(self):
        self.client.put_bytes(b"a", b"")
        self.client.call(Methods.Put, api_pb2.PutRequest(
            header=api_pb2.RequestHeader(key=b"b"),
            value=data_pb2.Value(integer=0)))

        get_resp = self.client.call(Methods.Get, api_pb2.GetRequest(
            header=api_pb2.RequestHeader(key=b"a")))
        self.assertTrue(get_resp.value.HasField('bytes'))
        self.assertFalse(get_resp.value.HasField('integer'))
        self.assertEqual(get_resp.value.bytes, b"")
        get_resp = self.client.call(Methods.Get, api_pb2.GetRequest(
            header=api_pb2.RequestHeader(key=b"b")))
        self.assertFalse(get_resp.value.HasField('bytes'))
        self.assertTrue(get_resp.value.HasField('integer'))
        self.assertEqual(get_resp.value.integer, 0)

    # Prepare a sequence of increment calls then flush them and verify the results.
    def test_prepare_and_flush(self):
        keys = []
        replies = []
        for i in range(10):
            key = ("key %02d" % i).encode('ascii')
            keys.append(key)
            reply = api_pb2.IncrementResponse()
            replies.append(reply)
            self.client.prepare(
                Methods.Increment,
                api_pb2.IncrementRequest(header=api_pb2.RequestHeader(key=key), increment=i),
                reply)

        self.client.flush()

        for i, reply in enumerate(replies):
            self.assertEqual(i, reply.new_value)

        # Now try 2 scans.
        scan1 = api_pb2.ScanResponse()
        scan2 = api_pb2.ScanResponse()
        self.client.prepare(
            Methods.Scan, api_pb2.ScanRequest(header=api_pb2.RequestHeader(
                key=b"key 00", end_key=b"key 05")), scan1)
        self.client.prepare(
            Methods.Scan, api_pb2.ScanRequest(header=api_pb2.RequestHeader(
                key=b"key 05", end_key=b"key 10")), scan2)

        self.client.flush()

        self.assertEqual(len(scan1.rows), 5)
        self.assertEqual(len(scan2.rows), 5)
        for i in range(5):
            self.assertEqual(scan1.rows[i].key, keys[i])
            self.assertEqual(scan1.rows[i].value.integer, i)

            self.assertEqual(scan2.rows[i].key, keys[i+5])
            self.assertEqual(scan2.rows[i].value.integer, i+5)

    # This is an example for using the call() method to Put and then get a value for a
    # given key.
    # TODO: In the go version this is an example test; when we have docs consider making it
    # a doctest.
    def test_example_call(self):
        key = b"a"
        value = b"asdf"

        # Store test value.
        self.client.call(Methods.Put,
                         api_pb2.PutRequest(header=api_pb2.RequestHeader(key=key),
                                            value=data_pb2.Value(bytes=value)))

        # Retrieve test value using the same key.
        get_resp = self.client.call(Methods.Get,
                                    api_pb2.GetRequest(header=api_pb2.RequestHeader(key=key)))

        self.assertTrue(get_resp.HasField('value'))
        self.assertEqual(get_resp.value.bytes, value)

    # This is an example for using the prepare() method to submit multiple key value
    # API operations to be run in parallel. Flush() is then used to begin execution of all
    # the prepared operations.
    # TODO: In the go version this is an example test; when we have docs consider making
    # it a doctest.
    def test_example_prepare(self):
        batch_size = 12
        keys = []
        values = []
        for i in range(batch_size):
            keys.append(("key-%03d" % i).encode('ascii'))
            values.append(("value-%0d3" % i).encode('ascii'))

            self.client.prepare(
                Methods.Put, api_pb2.PutRequest(header=api_pb2.RequestHeader(
                    key=keys[i]), value=data_pb2.Value(bytes=values[i])),
                api_pb2.PutResponse())

        # Flush all puts for parallel execution.
        self.client.flush()

        # Scan for the newly inserted rows in parallel.
        num_scans = 3
        rows_per_scan = batch_size // num_scans
        scan_responses = []
        for i in range(num_scans):
            first_key = keys[i*rows_per_scan]
            last_key = keys[((i+1)*rows_per_scan)-1]
            reply = api_pb2.ScanResponse()
            scan_responses.append(reply)
            self.client.prepare(
                Methods.Scan, api_pb2.ScanRequest(
                    header=api_pb2.RequestHeader(key=first_key, end_key=last_key+b"\x00"),
                    max_results=rows_per_scan),
                reply)

        # Flush all scans for parallel execution.
        self.client.flush()

        # Check results.
        for i in range(num_scans):
            for j in range(rows_per_scan):
                row = scan_responses[i].rows[j]
                self.assertEqual(row.key, keys[i*rows_per_scan+j])
                self.assertEqual(row.value.bytes, values[i*rows_per_scan+j])

    # This is an example for using the run_transaction method to submit multiple key value
    # API operations inside a transaction.
    # TODO: In the go version this is an example test; when we have docs consider making
    # it a doctest.
    def test_example_run_transaction(self):
        # Create test data.
        num_pairs = 10
        keys = []
        values = []
        for i in range(num_pairs):
            keys.append(('testkey-%0d3' % i).encode('ascii'))
            values.append(('testvalue-%03d' % i).encode('ascii'))


        # Insert all KV pairs inside a transaction.
        def callback(txn):
            for i in range(num_pairs):
                txn.prepare(Methods.Put,
                            api_pb2.PutRequest(header=api_pb2.RequestHeader(key=keys[i]),
                                               value=data_pb2.Value(bytes=values[i])),
                            api_pb2.PutResponse())
            # Note that the KV client is flushed automatically on transaction commit.
            # Invoking flush after individual API methods is only required if the result
            # needs to be received to take conditional action.
        put_opts = TransactionOptions(name="example put")
        self.client.run_transaction(put_opts, callback)

        # Read back KV pairs inside a transaction.
        get_responses = []

        def callback(txn):
            for i in range(num_pairs):
                get_responses.append(api_pb2.GetResponse())
                txn.prepare(Methods.Get,
                            api_pb2.GetRequest(header=api_pb2.RequestHeader(key=keys[i])),
                            get_responses[-1])
        get_opts = TransactionOptions(name="example get")
        self.client.run_transaction(get_opts, callback)

        # Check results.
        for i, get_resp in enumerate(get_responses):
            self.assertTrue(get_resp.HasField('value'))
            self.assertEqual(get_resp.value.bytes, values[i])

    def concurrent_increments(self):
        """Start two threads in parallel, both of which read the integers stored
        at the other's key and add it onto their own. It is checked that the outcome is
        serializable, i.e. exactly one of the two threads (the later write) sees the
        previous write by the other.
        """
        start = threading.Barrier(2)
        end = threading.Barrier(2+1)

        for i in range(2):
            def func(i):
                try:
                    # Read the other key, write key i.
                    read_key = ("value-%d" % ((i+1) % 2)).encode('ascii')
                    write_key = ("value-%d" % i).encode('ascii')
                    # Wait until the other threads are running.
                    start.wait()

                    def callback(txn):
                        # Retrieve the other key.
                        gr = txn.call(Methods.Get, api_pb2.GetRequest(
                            header=api_pb2.RequestHeader(key=read_key)))
                        other_value = gr.value.integer

                        txn.call(Methods.Increment, api_pb2.IncrementRequest(
                            header=api_pb2.RequestHeader(key=write_key),
                            increment=1+other_value))
                    txn_opts = TransactionOptions(name='test-%d' % i)
                    self.client.run_transaction(txn_opts, callback)
                finally:
                    end.wait()
            self.executor.submit(func, i)
        # Wait for the threads to finish.
        end.wait()

        # Verify that both keys contain something and, more importantly, that one key
        # actually contains the value of the first writer and not only its own.
        total = 0
        results = []
        for i in range(2):
            read_key = ('value-%d' % i).encode('ascii')
            gr = self.client.call(
                Methods.Get, api_pb2.GetRequest(header=api_pb2.RequestHeader(key=read_key)))
            self.assertTrue(gr.HasField('value'))
            self.assertTrue(gr.value.HasField('integer'))
            total += gr.value.integer
            results.append(gr.value.integer)

        # First writer should have 1, second one 2.
        self.assertEqual(total, 3, "got unserializable values %r" % results)


    # test_concurrent_increments is a simple explicit test for serializability
    # for the concrete situation described in:
    #  https://groups.google.com/forum/#!topic/cockroach-db/LdrC5_T0VNw
    def test_concurrent_increments(self):
        # Convenience loop: Crank up this number for testing this
        # more often. It'll increase test duration though.
        for i in range(5):
            self.client.call(Methods.DeleteRange,
                             api_pb2.DeleteRangeRequest(header=api_pb2.RequestHeader(
                                 key=b"value-0", end_key=b"value-1x")))
            self.concurrent_increments()
