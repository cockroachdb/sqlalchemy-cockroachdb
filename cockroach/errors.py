class ProtoError(Exception):
    """Common base class for all errors derived from Cockroach protobufs."""
    def __init__(self, proto):
        self.proto = proto


class GenericError(ProtoError):
    def __str__(self):
        return self.proto.message


class NotLeaderError(ProtoError):
    def __str__(self):
        return "range not leader; leader is %r" % self.proto.leader


class RangeNotFoundError(ProtoError):
    def __str__(self):
        return "range %d was not found" % self.proto.raft_id


class RangeKeyMismatchError(ProtoError):
    def __str__(self):
        if self.proto.HasField('range'):
            return ("key range %r-%r outside of bounds of range %r-%r" %
                    (self.proto.request_start_key, self.proto.request_end_key,
                     self.proto.range.start_key, self.proto.range.end_key))
        return ("key range %r-%r could not be located within a range on store" %
                (self.proto.request_start_key, self.proto.request_end_key))


class TransactionAbortedError(ProtoError):
    def __str__(self):
        return "txn aborted %s" % self.proto.txn


class TransactionPushError(ProtoError):
    def __str__(self):
        if not self.proto.HasField('txn'):
            return "failed to push %s" % self.proto.pushee_txn
        return "txn %s failed to push %s" % (self.proto.txn, self.proto.pushee_txn)


class TransactionRetryError(ProtoError):
    def __str__(self):
        return "retry txn %s" % self.proto.txn


class TransactionStatusError(ProtoError):
    def __str__(self):
        return "txn %s: %s" % (self.proto.txn, self.proto.msg)


class WriteIntentError(ProtoError):
    def __str__(self):
        return ("conflicting write intent at key %r from transaction %s: resolved? %r" %
                (self.proto.key, self.proto.txn, self.proto.resolved))


class WriteTooOldError(ProtoError):
    def __str__(self):
        return "write too old: timestamp %s < %s" % (self.proto.timestamp,
                                                     self.proto.existing_timestamp)


class ReadWithinUncertaintyIntervalError(ProtoError):
    def __str__(self):
        return ("read at time %s encountered previous write with "
                "future timestamp %s within uncertainty interval" % (
                    self.proto.timestamp, self.proto.existing_timestamp))


class OpRequiresTxnError(ProtoError):
    def __str__(self):
        return "the operation requires transactional context"


class ConditionFailedError(ProtoError):
    def __str__(self):
        return "unexpected value: %s" % self.proto.actual_value


class UnknownProtoError(ProtoError):
    def __str__(self):
        return "unknown proto error: %s" % self.proto


def raise_from_header(header):
    """If the given ResponseHeader proto has an error, raise it."""
    if not header.HasField('error'):
        return
    error = header.error
    if error.HasField('generic'):
        raise GenericError(error.generic)
    elif error.HasField('not_leader'):
        raise NotLeaderError(error.not_leader)
    elif error.HasField('range_not_found'):
        raise RangeNotFoundError(error.range_not_found)
    elif error.HasField('range_key_mismatch'):
        raise RangeKeyMismatchError(error.range_key_mismatch)
    elif error.HasField('read_within_uncertainty_interval'):
        raise ReadWithinUncertaintyIntervalError(error.read_within_uncertainty_interval)
    elif error.HasField('transaction_aborted'):
        raise TransactionAbortedError(error.transaction_aborted)
    elif error.HasField('transaction_push'):
        raise TransactionPushError(error.transaction_push)
    elif error.HasField('transaction_retry'):
        raise TransactionRetryError(error.transaction_retry)
    elif error.HasField('transaction_status'):
        raise TransactionStatusError(error.transaction_status)
    elif error.HasField('write_intent'):
        raise WriteIntentError(error.write_intent)
    elif error.HasField('write_too_old'):
        raise WriteTooOldError(error.write_too_old)
    elif error.HasField('op_requires_txn'):
        raise OpRequiresTxnError(error.op_requires_txn)
    elif error.HasField('condition_failed'):
        raise ConditionFailedError(error.condition_failed)
    else:
        raise UnknownProtoError(error)
