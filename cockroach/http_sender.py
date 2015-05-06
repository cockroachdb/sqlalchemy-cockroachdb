import datetime
import logging
import requests

from cockroach.interface import KVSender
from cockroach.util import RetryOptions, retry_with_backoff, RetryStatus

# URL path prefix which accepts incoming HTTP requests for the KV API.
kv_db_endpoint = "/kv/db/"
# Scheme for connecting to kv_db_endpoint. TODO: change to constant https.
kv_db_scheme = "http"

http_retry_options = RetryOptions(backoff=datetime.timedelta(milliseconds=50),
                                  max_backoff=datetime.timedelta(seconds=5),
                                  constant=2,
                                  max_attempts=0)  # retry indefinitely


class HTTPSender(KVSender):
    """Implementation of KVSender which exposes the key-value database
    provided by a Cockroach cluster by connecting via HTTP to a Cockroach node.
    Overly-busy nodes will redirect this client to other nodes.
    """
    def __init__(self, server):
        self.server = server  # host:port address of the Cockroach gateway node.
        self.session = requests.Session()

    def send(self, call):
        """Send call to Cockroach via an HTTP post.

        HTTP response codes which are retryable are retried with
        backoff in a loop using the default retry options. Other
        errors sending HTTP request are retried indefinitely using the
        same client command ID to avoid reporting failure when in fact
        the command may have gone through and been executed
        successfully. We retry here to eventually get through with the
        same client command ID and be given the cached response.
        """
        retry_opts = http_retry_options.copy()
        retry_opts.tag = "http %s" % call.method.name

        def retryable():
            try:
                resp = self._post(call)
            except Exception:
                # Assume all errors sending request are retryable.
                logging.warning("failed to send HTTP request or read its response", exc_info=True)
                return RetryStatus.CONTINUE
            else:
                if resp.status_code != 200:
                    logging.warning("failed to send HTTP request with status code %d",
                                    resp.status_code)
                    # See if we can retry based on HTTP response code.
                    if resp.status_code in (429, 503, 504):
                        # Retry on service unavailable and request timeout.
                        # TODO: respect the Retry-After header if present.
                        return RetryStatus.CONTINUE
                    else:
                        resp.raise_for_status()
                else:
                    # On a successful ost, we're done with retry loop.
                    return RetryStatus.BREAK
        try:
            retry_with_backoff(retry_opts, retryable)
        except Exception as e:
            # TODO: Are there any non-generic errors we need to handle here?
            # Is it better to let exceptions escape instead of stuffing them into
            # the reply?
            call.reply.header.error.message = str(e)
        return call.reply

    def close(self):
        self.session.close()

    def _post(self, call):
        """Post the call using the HTTP client.

        The call's method is appended to kv_db_endpoint and set as the
        URL path. The call's arguments are protobuf-serialized and
        written as the POST body. The content type is set to
        application/x-protobuf.

        On success, the response body is unmarshalled into call.reply.
        """
        body = call.args.SerializeToString()

        url = '%s://%s%s%s' % (kv_db_scheme, self.server, kv_db_endpoint, call.method.name)
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Accept': 'application/x-protobuf',
        }
        resp = self.session.post(url, headers=headers, data=body)
        if resp.status_code == 200:
            call.reply.ParseFromString(resp.content)
        return resp
