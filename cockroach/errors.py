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
    detail = header.error.detail
    if detail.HasField('not_leader'):
        raise NotLeaderError(detail.not_leader)
    elif detail.HasField('range_not_found'):
        raise RangeNotFoundError(detail.range_not_found)
    elif detail.HasField('range_key_mismatch'):
        raise RangeKeyMismatchError(detail.range_key_mismatch)
    elif detail.HasField('read_within_uncertainty_interval'):
        raise ReadWithinUncertaintyIntervalError(detail.read_within_uncertainty_interval)
    elif detail.HasField('transaction_aborted'):
        raise TransactionAbortedError(detail.transaction_aborted)
    elif detail.HasField('transaction_push'):
        raise TransactionPushError(detail.transaction_push)
    elif detail.HasField('transaction_retry'):
        raise TransactionRetryError(detail.transaction_retry)
    elif detail.HasField('transaction_status'):
        raise TransactionStatusError(detail.transaction_status)
    elif detail.HasField('write_intent'):
        raise WriteIntentError(detail.write_intent)
    elif detail.HasField('write_too_old'):
        raise WriteTooOldError(detail.write_too_old)
    elif detail.HasField('op_requires_txn'):
        raise OpRequiresTxnError(detail.op_requires_txn)
    elif detail.HasField('condition_failed'):
        raise ConditionFailedError(detail.condition_failed)
    else:
        raise GenericError(header.error)
