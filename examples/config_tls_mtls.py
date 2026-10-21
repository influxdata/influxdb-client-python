import ssl

from influxdb_client import InfluxDBClient

path_client_cert_file = "path/to/cert_file.crt"
path_client_key_file = "path/to/cert_file.key"
path_client_cert_file_pem = "path/to/cert_file.pem"

path_server_cert = "path/to/server_cert.crt"

# Config mTLS with client send certificate to the server with separated .crt file and .key file.
# `ssl_ca_cert` is optional.
with InfluxDBClient("https://localhost:8081",
                    token="my-token",
                    verify_ssl=True,
                    ssl_ca_cert=path_server_cert,
                    cert_file=path_client_cert_file,
                    cert_key_file=path_client_key_file, ) as client:
    client.version()

# Config mTLS with client send a certificate to the server with both certificate and key bundled inside a .pem file.
# `ssl_ca_cert` is optional.
with InfluxDBClient("https://localhost:8081",
                    token="my-token",
                    verify_ssl=True,
                    ssl_ca_cert=path_server_cert,
                    cert_file=path_client_cert_file_pem) as client:
    client.version()

# Configure TLS with SSLContext; omit cert_file, cert_key_file and ssl_ca_cert when supplying a configured context.
ssl_context = ssl.create_default_context(cafile=path_server_cert)
with InfluxDBClient("https://localhost:8081",
                    token="my-token",
                    verify_ssl=True,
                    ssl_context=ssl_context) as client:
    client.version()
