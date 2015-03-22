from cockroach.call import Call
from cockroach.methods import Methods
from cockroach.proto import api_pb2


class KV(object):
    """Key-value store client.

    KV provides serial access to a KV store via Call and parallel
    access via Prepare and Flush. A KV instance is not thread safe.
    """
    def __init__(self, sender, user='', user_priority=0):
        self.sender = sender
        # The default user to set on API calls. If user is set to
        # non-empty in call arguments, this value is ignored.
        self.user = user
        # The default user priority to set on API calls. If
        # user_priority is set non-zero in call arguments, this value
        # is ignored.
        self.user_priority = user_priority

        self.prepared = []

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
        self.sender.send(call)
        if call.reply.header.HasField('error'):
            # TODO: handle all error types
            raise Exception(call.reply.header.error.generic.message)
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
