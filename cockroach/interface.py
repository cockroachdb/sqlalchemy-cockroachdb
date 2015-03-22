from cockroach.proto import data_pb2


class TransactionOptions(object):
    def __init__(self, name='', isolation=data_pb2.SERIALIZABLE):
        self.name = name  # concise description of txn for debugging.
        self.isolation = isolation


class KVSender(object):
    """Interface for sending a request to a key-value database backend."""
    def send(self, call):
        """Invokes call.method with call.args and sets the result in call.reply."""
        raise NotImplementedError()

    def close(self):
        """Frees up resources in use by the sender."""
        pass
