"""Integration tests for mTLS connectivity."""
import datetime
import json

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from testdatapy.config.loader import AppConfig
from testdatapy.producers import JsonProducer


class TestMTLSIntegration:
    """Test mTLS connectivity with Kafka."""

    @pytest.fixture
    def cert_dir(self, tmp_path):
        """Create temporary directory for certificates."""
        cert_dir = tmp_path / "certs"
        cert_dir.mkdir()
        return cert_dir

    @pytest.fixture
    def generate_test_certs(self, cert_dir):
        """Generate test certificates for mTLS."""
        # Generate CA key and certificate
        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        ca_cert = (
            x509.CertificateBuilder()
            .subject_name(
                x509.Name([
                    x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Test CA"),
                    x509.NameAttribute(NameOID.COMMON_NAME, "Test CA"),
                ])
            )
            .issuer_name(
                x509.Name([
                    x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Test CA"),
                    x509.NameAttribute(NameOID.COMMON_NAME, "Test CA"),
                ])
            )
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.utcnow())
            .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=None),
                critical=True,
            )
            .sign(ca_key, hashes.SHA256())
        )

        # Generate client key and certificate
        client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        client_cert = (
            x509.CertificateBuilder()
            .subject_name(
                x509.Name([
                    x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                    x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Test Client"),
                    x509.NameAttribute(NameOID.COMMON_NAME, "Test Client"),
                ])
            )
            .issuer_name(ca_cert.subject)
            .public_key(client_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.utcnow())
            .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=365))
            .sign(ca_key, hashes.SHA256())
        )

        # Save certificates and keys
        ca_cert_path = cert_dir / "ca-cert.pem"
        client_cert_path = cert_dir / "client-cert.pem"
        client_key_path = cert_dir / "client-key.pem"

        with open(ca_cert_path, "wb") as f:
            f.write(ca_cert.public_bytes(serialization.Encoding.PEM))

        with open(client_cert_path, "wb") as f:
            f.write(client_cert.public_bytes(serialization.Encoding.PEM))

        with open(client_key_path, "wb") as f:
            f.write(
                client_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        return {
            "ca_cert": str(ca_cert_path),
            "client_cert": str(client_cert_path),
            "client_key": str(client_key_path),
        }

    def test_mtls_configuration(self, generate_test_certs):
        """Test mTLS configuration loading."""
        config = AppConfig(
            kafka={
                "bootstrap_servers": "localhost:9093",
                "security_protocol": "SSL",
                "ssl_ca_location": generate_test_certs["ca_cert"],
                "ssl_certificate_location": generate_test_certs["client_cert"],
                "ssl_key_location": generate_test_certs["client_key"],
            }
        )

        confluent_config = config.to_confluent_config()
        
        assert confluent_config["security.protocol"] == "SSL"
        assert confluent_config["ssl.ca.location"] == generate_test_certs["ca_cert"]
        assert confluent_config["ssl.certificate.location"] == generate_test_certs["client_cert"]
        assert confluent_config["ssl.key.location"] == generate_test_certs["client_key"]

    @pytest.mark.skip(reason="Requires Kafka with SSL enabled")
    def test_mtls_producer_connection(self, generate_test_certs):
        """Test producer with mTLS connection."""
        # This test requires a Kafka broker configured with SSL
        producer = JsonProducer(
            bootstrap_servers="localhost:9093",
            topic="test-mtls",
            config={
                "security.protocol": "SSL",
                "ssl.ca.location": generate_test_certs["ca_cert"],
                "ssl.certificate.location": generate_test_certs["client_cert"],
                "ssl.key.location": generate_test_certs["client_key"],
            }
        )

        # Test producing a message
        producer.produce(key="test", value={"message": "mTLS test"})
        producer.flush()

    def test_mtls_config_from_file(self, tmp_path, generate_test_certs):
        """Test loading mTLS config from file."""
        config_data = {
            "bootstrap.servers": "localhost:9093",
            "security.protocol": "SSL",
            "ssl.ca.location": generate_test_certs["ca_cert"],
            "ssl.certificate.location": generate_test_certs["client_cert"],
            "ssl.key.location": generate_test_certs["client_key"],
        }

        config_file = tmp_path / "mtls_config.json"
        config_file.write_text(json.dumps(config_data))

        config = AppConfig.from_file(str(config_file))
        
        assert config.kafka.security_protocol == "SSL"
        assert config.kafka.ssl_ca_location == generate_test_certs["ca_cert"]

    def test_mtls_with_password(self, tmp_path, cert_dir):
        """Test mTLS configuration with password-protected key."""
        # Generate password-protected key
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        encrypted_key_path = cert_dir / "encrypted-key.pem"
        
        with open(encrypted_key_path, "wb") as f:
            f.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.BestAvailableEncryption(b"testpassword"),
                )
            )

        config = AppConfig(
            kafka={
                "bootstrap_servers": "localhost:9093",
                "security_protocol": "SSL",
                "ssl_key_location": str(encrypted_key_path),
                "ssl_key_password": "testpassword",
            }
        )

        confluent_config = config.to_confluent_config()
        assert confluent_config["ssl.key.password"] == "testpassword"
