import datetime
import logging

from cockroach.call import Call
from cockroach import errors
from cockroach.methods import Methods
from cockroach.proto import api_pb2
from cockroach.txn_sender import TxnSender
from cockroach import util

txn_retry_options = util.RetryOptions(
    backoff=datetime.timedelta(milliseconds=50),
    max_backoff=datetime.timedelta(seconds=2),
    constant=2,
    max_attempts=0)  # retry indefinitely

class KV(object):
    """Key-value store client.

    KV provides serial access to a KV store via Call and parallel
    access via Prepare and Flush. A KV instance is not thread safe.
    """
    def __init__(self, sender, user='', user_priority=0):
        self._sender = sender
        # The default user to set on API calls. If user is set to
        # non-empty in call arguments, this value is ignored.
        self.user = user
        # The default user priority to set on API calls. If
        # user_priority is set non-zero in call arguments, this value
        # is ignored.
        self.user_priority = user_priority

        self.prepared = []

    def sender(self):
        """Returns the sender supplied to KV, unless wrapped by a
        transactional sender, in which case it returns the unwrapped sender.
        """
        if isinstance(self._sender, TxnSender):
            return self._sender.wrapped
        return self._sender

    def call(self, method, args, reply=None):
        """Call invokes the KV command synchronously and returns the response.

        If preceeding calls have been made to prepare() without a call
        to flush(), this call is prepared and then all prepared calls
        are flushed.
        """
        if len(self.prepared) > 0:
            if reply is None:
                reply = method.response_type()
            self.prepare(method, args, reply)
            self.flush()
            return reply
        if not args.header.user:
            args.header.user = self.user
        if not args.header.HasField('user_priority') and self.user_priority != 0:
            args.header.user_priority = self.user_priority
        call = Call(method, args, reply)
        call.reset_client_cmd_id()
        self._sender.send(call)
        errors.raise_from_header(call.reply.header)
        return call.reply

    def prepare(self, method, args, reply):
        """Prepare accepts a KV API call to be called later.

        The call will be buffered locally until the first call to
        flush(), at which time it will be sent for execution as part
        of a batch call. Using prepare/flush parallelizes queries and
        updates and should be used where possible for efficiency.

        For clients using an HTTP sender, prepare/flush allows multiple
        commands to be sent over the same connection. For transactional
        clients, prepare/flush can dramatically improve efficiency by
        compressing multiple writes into a single atomic update in the
        event that the writes are to keys within a single range. However,
        using prepare/flush alone will not guarantee atomicity. Clients
        must use a transaction for that purpose.

        The supplied reply struct will not be valid until after a call
        to flush().
        """
        call = Call(method, args, reply)
        call.reset_client_cmd_id()
        self.prepared.append(call)

    def flush(self):
        """Flush sends all previously prepared calls.

        The calls are organized into a single batch command and sent
        together. Flush raises the first error, if any, where calls
        are executed in the order in which they were prepared.  After
        Flush returns, all prepared reply structs will be valid.
        """
        if len(self.prepared) == 0:
            return
        elif len(self.prepared) == 1:
            call = self.prepared[0]
            self.prepared = []
            self.call(call.method, call.args, call.reply)
            return
        batch_args = api_pb2.BatchRequest()
        calls = self.prepared
        self.prepared = []
        for call in calls:
            if not batch_args.header.HasField('key'):
                # The batch inherits the key range of the first request added.
                # TODO: batches should include a list of key ranges representing
                # the constituent requests.
                batch_args.header.key = call.args.header.key
                batch_args.header.end_key = call.args.header.end_key
            req_union = batch_args.requests.add()
            # TODO: this should be a proto 'oneof'.
            getattr(req_union, call.method.name.lower()).CopyFrom(call.args)
        batch_reply = self.call(Methods.Batch, batch_args)
        for call, response in zip(calls, batch_reply.responses):
            call.reply.CopyFrom(getattr(response, call.method.name.lower()))

    def run_transaction(self, opts, retryable):
        """RunTransaction executes retryable in the context of a distributed transaction.

        The ``retryable`` argument is a function which takes one parameter, a `KV`
        object.

        The transaction is automatically aborted if retryable
        returns any error aside from recoverable internal errors, and is
        automatically committed otherwise. retryable should have no side
        effects which could cause problems in the event it must be run more
        than once. The opts struct contains transaction settings.

        Calling run_transaction on the transactional KV client which is
        supplied to the retryable function is an error.
        """
        if isinstance(self._sender, TxnSender):
            raise Exception("cannot invoke run_transaction on an already-transactional client")

        # Create a new KV for the transaction using a transactional KV sender.
        txn_sender = TxnSender(self._sender, opts)
        txn_kv = KV(txn_sender, user=self.user, user_priority=self.user_priority)

        # Run retriable in a loop until we encounter a success or error condition this
        # loop isn't capable of handling.
        retry_opts = txn_retry_options.copy()
        retry_opts.tag = opts.name

        def callback():
            txn_sender.txn_end = False  # always reset before [re]starting txn
            try:
                retryable(txn_kv)
            except errors.ReadWithinUncertaintyIntervalError:
                # Retry immediately on read within uncertainty interval.
                return util.RetryStatus.RESET
            except errors.TransactionAbortedError:
                # If the transaction was aborted, the TxnSender will have created
                # a new txn. We allow backoff/retry in this case.
                return util.RetryStatus.CONTINUE
            except errors.TransactionPushError:
                # Backoff and retry on failure to push a conflicting transaction.
                return util.RetryStatus.CONTINUE
            except errors.TransactionRetryError:
                # Return RESET for an immediate retry (as in the case of an SSI txn
                # whose timestamp was pushed.
                return util.RetryStatus.RESET
            # All other errors are allowed to escape, aborting the retry loop.
            if not txn_sender.txn_end:
                # If there were no errors running retryable, commit the txn.
                # This may block waiting for outstanding writes to complete in
                # case retryable didn't -- we need the most recent of all response
                # timestamps in order to commit.
                txn_kv.call(Methods.EndTransaction, api_pb2.EndTransactionRequest(commit=True))
            return util.RetryStatus.BREAK
        try:
            util.retry_with_backoff(retry_opts, callback)
        except Exception as e:
            if not txn_sender.txn_end:
                try:
                    txn_kv.call(Methods.EndTransaction,
                                api_pb2.EndTransactionRequest(commit=False))
                except Exception:
                    logging.error("failure aborting transaction; abort caused by %s",
                                  e, exc_info=True)
            raise
