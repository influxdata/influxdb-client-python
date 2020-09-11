import http.server
import json
import os
import threading
import unittest

from influxdb_client import InfluxDBClient


class InfluxDBClientTest(unittest.TestCase):

    def tearDown(self) -> None:
        if self.client:
            self.client.close()
        if hasattr(self, 'httpd'):
            self.httpd.shutdown()
        if hasattr(self, 'httpd_thread'):
            self.httpd_thread.join()

    def test_TrailingSlashInUrl(self):
        self.client = InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org")
        self.assertEqual('http://localhost:8086', self.client.api_client.configuration.host)

        self.client = InfluxDBClient(url="http://localhost:8086/", token="my-token", org="my-org")
        self.assertEqual('http://localhost:8086', self.client.api_client.configuration.host)

    def test_ConnectToSelfSignedServer(self):
        import http.server
        import ssl

        # Disable unverified HTTPS requests
        import urllib3
        urllib3.disable_warnings()

        # Configure HTTP server
        self.httpd = http.server.HTTPServer(('localhost', 0), ServerWithSelfSingedSSL)
        self.httpd.socket = ssl.wrap_socket(self.httpd.socket, certfile=f'{os.path.dirname(__file__)}/server.pem',
                                            server_side=True)

        # Start server at background
        self.httpd_thread = threading.Thread(target=self.httpd.serve_forever)
        self.httpd_thread.start()

        self.client = InfluxDBClient(f"https://localhost:{self.httpd.server_address[1]}",
                                     token="my-token", verify_ssl=False)
        health = self.client.health()

        self.assertEqual(health.message, 'ready for queries and writes')
        self.assertEqual(health.status, "pass")
        self.assertEqual(health.name, "influxdb")

    def test_init_from_file_ssl_default(self):
        self.client = InfluxDBClient.from_config_file(f'{os.path.dirname(__file__)}/config.ini')

        self.assertTrue(self.client.api_client.configuration.verify_ssl)

    def test_init_from_file_ssl(self):
        self.client = InfluxDBClient.from_config_file(f'{os.path.dirname(__file__)}/config-disabled-ssl.ini')

        self.assertFalse(self.client.api_client.configuration.verify_ssl)

    def test_init_from_env_ssl_default(self):
        del os.environ["INFLUXDB_V2_VERIFY_SSL"]
        self.client = InfluxDBClient.from_env_properties()

        self.assertTrue(self.client.api_client.configuration.verify_ssl)

    def test_init_from_env_ssl(self):
        os.environ["INFLUXDB_V2_VERIFY_SSL"] = "False"
        self.client = InfluxDBClient.from_env_properties()

        self.assertFalse(self.client.api_client.configuration.verify_ssl)


class ServerWithSelfSingedSSL(http.server.SimpleHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        response = json.dumps(
            dict(name="influxdb", message="ready for queries and writes", status="pass", checks=[], version="2.0.0",
                 commit="abcdefgh")).encode('utf-8')
        self.wfile.write(response)
