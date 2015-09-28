import threading
import unittest
from werkzeug.wrappers import Request, Response
import wsgiref.simple_server

from cockroach.http_sender import HTTPSender
from cockroach.sql.driver import wire_pb2

test_sql = "SELECT 1"
test_request = wire_pb2.Request(
    sql=test_sql,
)
test_session = b"new_session"
test_response = wire_pb2.Response(
    session=test_session,
)

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
            self.assertEqual(req.path, "/sql/Execute")
            args = wire_pb2.Request()
            args.ParseFromString(req.data)
            self.assertEqual(args.sql, test_sql)
            resp = Response(test_response.SerializeToString(),
                            content_type="application/x-protobuf")
            return resp(environ, start_response)
        self.start_server(handler)
        reply = self.sender.send(test_request)
        self.assertEqual(reply.session, test_session)

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
                    resp = Response(test_response.SerializeToString(),
                                    content_type="application/x-protobuf")
                return resp(environ, start_response)
            self.start_server(handler)
            err = None
            try:
                reply = self.sender.send(test_request)
            except Exception as e:
                err = e
            finally:
                self.stop_server()
            if retry:
                self.assertEqual(count[0], 2, "expected retry for code %d; count=%d" % (
                    code, count[0]))
                self.assertIsNone(err,
                                  "expected success after retry for code %d; got %s" % (
                                      code, err))
                self.assertEqual(reply.session, test_session)
            else:
                self.assertEqual(count[0], 1, "expected no retry for code %d" % code)
                self.assertIsNotNone(err, "expected error")

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
                resp = Response(test_response.SerializeToString(),
                                content_type="application/x-protobuf")
            return resp(environ, start_response)
        self.start_server(handler)
        reply = self.sender.send(test_request)
        self.assertEqual(count[0], 2)
        self.assertEqual(reply.session, test_session)
