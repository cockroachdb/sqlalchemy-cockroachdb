import random
import time


class Call(object):
    def __init__(self, method, args, reply=None):
        self.method = method
        self.args = args
        if reply is not None:
            self.reply = reply
        else:
            self.reply = method.response_type()


    def reset_client_cmd_id(self):
        """Set the client command ID if the call is for a read-write method.

        The client command ID provides idempotency protection in conjunction
        with the server.
        """
        if self.method.is_write:
            cmd_id = self.args.header.cmd_id
            cmd_id.wall_time = int(time.time() * 1e9)
            cmd_id.random = random.randint(0, 1 << 63)
