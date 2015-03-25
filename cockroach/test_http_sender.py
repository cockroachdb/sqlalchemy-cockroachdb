import threading
import unittest
from werkzeug.wrappers import Request, Response
import wsgiref.simple_server

from cockroach.call import Call
from cockroach.http_sender import HTTPSender
from cockroach.methods import Methods
from cockroach.proto import api_pb2, data_pb2

test_key = b"a"
test_ts = data_pb2.Timestamp(wall_time=1, logical=1)
test_put_request = api_pb2.PutRequest(
    header=api_pb2.RequestHeader(timestamp=test_ts, key=test_key))
test_put_response = api_pb2.PutResponse(header=api_pb2.ResponseHeader(timestamp=test_ts))


class HTTPSenderTest(unittest.TestCase):
    def start_server(self, handler):
        self.server = wsgiref.simple_server.make_server('127.0.0.1', 0, handler)
        self.addr = '127.0.0.1:%d' % self.server.server_port
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.sender = HTTPSender(self.addr)

    def stop_server(self):
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join()
            self.sender.close()
            self.server = None
            self.thread = None
            self.sender = None

    def setUp(self):
        self.server = None

    def tearDown(self):
        self.stop_server()

    # Verify sending posts.
    def test_send(self):
        def handler(environ, start_response):
            req = Request(environ)
            self.assertEqual(req.method, "POST")
            self.assertEqual(req.path, "/kv/db/Put")
            args = api_pb2.PutRequest()
            args.ParseFromString(req.data)
            self.assertEqual(args.header.key, test_key)
            resp = Response(test_put_response.SerializeToString(),
                            content_type="application/x-protobuf")
            return resp(environ, start_response)
        self.start_server(handler)
        reply = self.sender.send(Call(Methods.Put, test_put_request))
        self.assertFalse(reply.header.HasField('error'))
        self.assertEqual(reply.header.timestamp, test_ts)

    # Verify that send is retried on some HTTP response codes but not others.
    def test_retry_response_codes(self):
        test_cases = [
            (503, True),
            (504, True),
            (429, True),
            (401, False),
            (500, False),
            ]
        for code, retry in test_cases:
            count = [0]

            def handler(environ, start_response):
                count[0] += 1
                if count[0] == 1:
                    resp = Response("error message", status=code)
                else:
                    self.assertTrue(retry, "didn't expect retry on code %d" % code)
                    resp = Response(test_put_response.SerializeToString(),
                                    content_type="application/x-protobuf")
                return resp(environ, start_response)
            self.start_server(handler)
            try:
                reply = self.sender.send(Call(Methods.Put, test_put_request))
            finally:
                self.stop_server()
            if retry:
                self.assertEqual(count[0], 2, "expected retry for code %d; count=%d" % (
                    code, count[0]))
                self.assertFalse(reply.header.HasField('error'),
                                 "expected success after retry for code %d; got %s" % (
                                     code, reply.header.error))
            else:
                self.assertEqual(count[0], 1, "expected no retry for code %d" % code)
                self.assertTrue(reply.header.HasField('error'), "expected error")

    # Verify that send is retried on an unparseable response.
    # The go implementation also tests abruptly closed connections but we cannot
    # easily test that here.
    def test_retry_parse_error(self):
        count = [0]

        def handler(environ, start_response):
            count[0] += 1
            if count[0] == 1:
                # On first attempt, send a garbage response with HTTP 200.
                resp = Response(b'\xff\xfe\x23\x44',
                                content_type="application/x-protobuf")
            else:
                resp = Response(test_put_response.SerializeToString(),
                                content_type="application/x-protobuf")
            return resp(environ, start_response)
        self.start_server(handler)
        reply = self.sender.send(Call(Methods.Put, test_put_request))
        self.assertFalse(reply.header.HasField('error'))
        self.assertEqual(count[0], 2)
