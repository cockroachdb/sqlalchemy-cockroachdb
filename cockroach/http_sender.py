import datetime
import logging
import requests

from cockroach.util import RetryOptions, retry_with_backoff, RetryStatus
from cockroach.sql.driver import wire_pb2
from cockroach.sql.driver import errors

# URL path prefix which accepts incoming HTTP requests for the SQL API.
sql_endpoint = "/sql/Execute"
# Scheme for connecting to kv_db_endpoint. TODO: change to constant https.
sql_scheme = "http"

http_retry_options = RetryOptions(backoff=datetime.timedelta(milliseconds=50),
                                  max_backoff=datetime.timedelta(seconds=5),
                                  constant=2,
                                  max_attempts=0)  # retry indefinitely


class HTTPSender(object):
    """Implementation of KVSender which exposes the key-value database
    provided by a Cockroach cluster by connecting via HTTP to a Cockroach node.
    Overly-busy nodes will redirect this client to other nodes.
    """
    def __init__(self, server):
        self.server = server  # host:port address of the Cockroach gateway node.
        self.session = requests.Session()

    def send(self, args):
        """Send call to Cockroach via an HTTP post.

        HTTP response codes which are retryable are retried with
        backoff in a loop using the default retry options. Other
        errors sending HTTP request are retried indefinitely using the
        same client command ID to avoid reporting failure when in fact
        the command may have gone through and been executed
        successfully. We retry here to eventually get through with the
        same client command ID and be given the cached response.
        """
        reply = wire_pb2.Response()

        def retryable():
            try:
                http_resp = self._post(args)
            except Exception:
                # Assume all errors sending request are retryable.
                logging.warning("failed to send HTTP request", exc_info=True)
                return RetryStatus.CONTINUE
            else:
                if http_resp.status_code != 200:
                    logging.warning("failed to send HTTP request with status code %d",
                                    http_resp.status_code)
                    # See if we can retry based on HTTP response code.
                    if http_resp.status_code in (429, 503, 504):
                        # Retry on service unavailable and request timeout.
                        # TODO: respect the Retry-After header if present.
                        return RetryStatus.CONTINUE
                    else:
                        http_resp.raise_for_status()
                else:
                    try:
                        reply.ParseFromString(http_resp.content)
                    except Exception:
                        logging.warning("failed to parse response", exc_info=True)
                        return RetryStatus.CONTINUE
                    # On a successful post, we're done with retry loop.
                    return RetryStatus.BREAK
        retry_with_backoff(http_retry_options, retryable)
        for result in reply.results:
            if result.error:
                raise errors.DatabaseError(result.error)
        return reply

    def close(self):
        self.session.close()

    def _post(self, args):
        """Post the call using the HTTP client.

        The call's method is appended to kv_db_endpoint and set as the
        URL path. The call's arguments are protobuf-serialized and
        written as the POST body. The content type is set to
        application/x-protobuf.

        On success, the response body is unmarshalled into call.reply.
        """
        body = args.SerializeToString()

        url = '%s://%s%s' % (sql_scheme, self.server, sql_endpoint)
        headers = {
            'Content-Type': 'application/x-protobuf',
            'Accept': 'application/x-protobuf',
        }
        resp = self.session.post(url, headers=headers, data=body)
        return resp
