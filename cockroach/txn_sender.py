import collections

from cockroach.interface import KVSender
from cockroach.methods import Methods
from cockroach.proto import data_pb2

class TransactionOptions(object):
    def __init__(self, name='', isolation=data_pb2.SERIALIZABLE):
        self.name = name  # concise description of txn for debugging.
        self.isolation = isolation

class TxnSender(KVSender):
    """A TxnSender proxies requests to the underlying KVSender,
    automatically beginning a transaction and then propagating txn
    changes to all commands. On receipt of TransactionRetryError, the
    transaction epoch is incremented and error passed to caller. On
    receipt of TransactionAbortedError, the transaction is re-created
    and error passed to caller.

    TxnSender is not thread safe.
    """
    def __init__(self, wrapped, opts):
        self.wrapped = wrapped
        self.txn = data_pb2.Transaction(name=opts.name, isolation=opts.isolation)
        self.txn_end = False

    def send(self, call):
        call.args.header.txn.CopyFrom(self.txn)
        ret = self.wrapped.send(call)
        if call.reply.header.HasField('txn'):
            self._update_txn(call.reply.header.txn)
        if call.reply.header.error.HasField('transaction_aborted'):
            abort_err = call.reply.header.error.transaction_aborted
            # On Abort, reset the transaction so we start anew on restart.
            self.txn = data_pb2.Transaction(
                name=self.txn.name, isolation=self.txn.isolation,
                priority=abort_err.txn.priority)  # acts as minimum priority on restart.
        elif not call.reply.header.HasField('error'):
            # Check for whether the transaction was ended as a direct call or as part
            # of a batch.
            if call.method is Methods.EndTransaction:
                self.txn_end = True
            elif call.method is Methods.Batch:
                for req_union in call.args.requests:
                    if req_union.HasField('end_transaction'):
                        self.txn_end = True
                        break
        return ret

    def close(self):
        """Close is a noop for TxnSender.

        Note that the wrapped sender isn't closed because the TxnSender is closed
        immediately upon transaction completion and the wrapped sender is reused.
        """
        pass

    def _update_txn(self, other):
        """Ratchet priority, timestamp, and original timestamp values (among others
        for the transaction.

        If self.txn.id is empty, then the transaction is copied from other.
        """
        if not self.txn.id:
            self.txn.CopyFrom(other)
        if other.status != data_pb2.PENDING:
            self.txn.status = other.status
        if self.txn.epoch < other.epoch:
            self.txn.epoch = other.epoch
        if _timestamp_less(self.txn.timestamp, other.timestamp):
            self.txn.timestamp.CopyFrom(other.timestamp)
        if _timestamp_less(self.txn.orig_timestamp, other.orig_timestamp):
            self.txn.orig_timestamp.CopyFrom(other.orig_timestamp)
        # Should not actually change at the time of writing.
        self.txn.max_timestamp.CopyFrom(other.max_timestamp)
        # Copy the list of nodes without time uncertainty.
        self.txn.certain_nodes.nodes[:] = other.certain_nodes.nodes[:]
        if self.txn.priority < other.priority:
            self.txn.priority = other.priority

def _timestamp_less(a, b):
    return a.wall_time < b.wall_time or (a.wall_time == b.wall_time and a.logical < b.logical)
