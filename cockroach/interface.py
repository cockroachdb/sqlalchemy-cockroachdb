class KVSender(object):
    """Interface for sending a request to a key-value database backend."""
    def send(self, call):
        """Invokes call.method with call.args and sets the result in call.reply."""
        raise NotImplementedError()

    def close(self):
        """Frees up resources in use by the sender."""
        pass
