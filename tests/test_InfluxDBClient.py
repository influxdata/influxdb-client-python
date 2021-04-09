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

    def test_default_conf(self):
        self.client = InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org")
        self.assertIsNotNone(self.client.api_client.configuration.connection_pool_maxsize)

    def test_TrailingSlashInUrl(self):
        self.client = InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org")
        self.assertEqual('http://localhost:8086', self.client.api_client.configuration.host)

        self.client = InfluxDBClient(url="http://localhost:8086/", token="my-token", org="my-org")
        self.assertEqual('http://localhost:8086', self.client.api_client.configuration.host)

    def test_ConnectToSelfSignedServer(self):
        self._start_http_server()

        self.client = InfluxDBClient(f"https://localhost:{self.httpd.server_address[1]}",
                                     token="my-token", verify_ssl=False)
        health = self.client.health()

        self.assertEqual(health.message, 'ready for queries and writes')
        self.assertEqual(health.status, "pass")
        self.assertEqual(health.name, "influxdb")

    def test_certificate_file(self):
        self._start_http_server()

        self.client = InfluxDBClient(f"https://localhost:{self.httpd.server_address[1]}",
                                     token="my-token", verify_ssl=True,
                                     ssl_ca_cert=f'{os.path.dirname(__file__)}/server.pem')
        health = self.client.health()

        self.assertEqual(health.message, 'ready for queries and writes')
        self.assertEqual(health.status, "pass")
        self.assertEqual(health.name, "influxdb")

    def test_init_from_ini_file(self):
        self.client = InfluxDBClient.from_config_file(f'{os.path.dirname(__file__)}/config.ini')

        self.assertConfig()

    def test_init_from_toml_file(self):
        self.client = InfluxDBClient.from_config_file(f'{os.path.dirname(__file__)}/config.toml')

        self.assertConfig()

    def assertConfig(self):
        self.assertEqual("http://localhost:8086", self.client.url)
        self.assertEqual("my-org", self.client.org)
        self.assertEqual("my-token", self.client.token)
        self.assertEqual(6000, self.client.api_client.configuration.timeout)
        self.assertEqual(3, len(self.client.default_tags))
        self.assertEqual("132-987-655", self.client.default_tags["id"])
        self.assertEqual("California Miner", self.client.default_tags["customer"])
        self.assertEqual("${env.data_center}", self.client.default_tags["data_center"])
        self.assertEqual(55, self.client.api_client.configuration.connection_pool_maxsize)

    def test_init_from_file_ssl_default(self):
        self.client = InfluxDBClient.from_config_file(f'{os.path.dirname(__file__)}/config.ini')

        self.assertTrue(self.client.api_client.configuration.verify_ssl)

    def test_init_from_file_ssl(self):
        self.client = InfluxDBClient.from_config_file(f'{os.path.dirname(__file__)}/config-disabled-ssl.ini')

        self.assertFalse(self.client.api_client.configuration.verify_ssl)

    def test_init_from_env_ssl_default(self):
        if os.getenv("INFLUXDB_V2_VERIFY_SSL"):
            del os.environ["INFLUXDB_V2_VERIFY_SSL"]
        self.client = InfluxDBClient.from_env_properties()

        self.assertTrue(self.client.api_client.configuration.verify_ssl)

    def test_init_from_env_ssl(self):
        os.environ["INFLUXDB_V2_SSL_CA_CERT"] = "/my/custom/path"
        self.client = InfluxDBClient.from_env_properties()

        self.assertEqual("/my/custom/path", self.client.api_client.configuration.ssl_ca_cert)

    def test_init_from_file_ssl_ca_cert_default(self):
        self.client = InfluxDBClient.from_config_file(f'{os.path.dirname(__file__)}/config.ini')

        self.assertIsNone(self.client.api_client.configuration.ssl_ca_cert)

    def test_init_from_file_ssl_ca_cert(self):
        self.client = InfluxDBClient.from_config_file(f'{os.path.dirname(__file__)}/config-ssl-ca-cert.ini')

        self.assertEqual("/path/to/my/cert", self.client.api_client.configuration.ssl_ca_cert)

    def test_init_from_env_ssl_ca_cert_default(self):
        if os.getenv("INFLUXDB_V2_SSL_CA_CERT"):
            del os.environ["INFLUXDB_V2_SSL_CA_CERT"]
        self.client = InfluxDBClient.from_env_properties()

        self.assertIsNone(self.client.api_client.configuration.ssl_ca_cert)

    def test_init_from_env_ssl_ca_cert(self):
        os.environ["INFLUXDB_V2_SSL_CA_CERT"] = "/my/custom/path/to/cert"
        self.client = InfluxDBClient.from_env_properties()

        self.assertEqual("/my/custom/path/to/cert", self.client.api_client.configuration.ssl_ca_cert)

    def test_init_from_env_connection_pool_maxsize(self):
        os.environ["INFLUXDB_V2_CONNECTION_POOL_MAXSIZE"] = "29"
        self.client = InfluxDBClient.from_env_properties()

        self.assertEqual(29, self.client.api_client.configuration.connection_pool_maxsize)

    def _start_http_server(self):
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
