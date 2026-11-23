# make_cert.py
"""
Генерация self-signed CA + серверного сертификата для HTTPS по IP.

ЗАЧЕМ:
  Let's Encrypt не выдаёт сертификаты на голый IP (только на домены).
  Поэтому создаём свой локальный CA и подписываем им серверный
  сертификат с SAN=IP:62.113.115.185.

  ca.pem вшивается в КЛИЕНТ (ValueDetector). Клиент доверяет ТОЛЬКО
  этому CA — значит подменить трафик (MITM) нельзя, даже по HTTP-порту.

Что создаётся (в папке certs/):
  ca.key      — приватный ключ CA (НИКОМУ не отдавать, на сервере)
  ca.pem      — публичный сертификат CA (этот файл → в клиент!)
  server.key  — приватный ключ сервера (на сервере)
  server.crt  — сертификат сервера (на сервере)

Использование:
    python make_cert.py 62.113.115.185

После генерации:
  1. Скопировать ca.pem в клиент:  PGFrontPatch/certs/ca.pem
  2. В .env сервера добавить:
        SSL_CERTFILE=<abs path>/certs/server.crt
        SSL_KEYFILE=<abs path>/certs/server.key
  3. В клиенте config.json:
        backend_url = https://62.113.115.185:8000
        ws_url      = wss://62.113.115.185:8000/ws
"""
import sys
import os
import datetime
import ipaddress
from pathlib import Path

from cryptography import x509
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

CERTS_DIR = Path(__file__).resolve().parent / "certs"

CA_DAYS = 3650        # 10 лет
SERVER_DAYS = 825     # ~2 года (лимит браузеров)


def _write(path: Path, data: bytes):
    path.write_bytes(data)
    print(f"  ✔ {path.name} ({len(data)} байт)")


def generate(ip: str, extra_hosts: list = None):
    CERTS_DIR.mkdir(exist_ok=True)

    print(f"\n[1/3] Генерация CA...")
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    ca_name = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "RU"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ValueDetector Pro"),
        x509.NameAttribute(NameOID.COMMON_NAME, "ValueDetector Pro Local CA"),
    ])
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=CA_DAYS))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False,
                key_encipherment=False, data_encipherment=False,
                key_agreement=False, key_cert_sign=True, crl_sign=True,
                encipher_only=False, decipher_only=False,
            ), critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )

    _write(CERTS_DIR / "ca.key", ca_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    _write(CERTS_DIR / "ca.pem", ca_cert.public_bytes(serialization.Encoding.PEM))

    print(f"[2/3] Генерация серверного сертификата для {ip}...")
    srv_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # SAN: IP + localhost, чтобы работало и локально
    san_entries = [
        x509.DNSName("localhost"),
        x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
    ]
    try:
        san_entries.insert(0, x509.IPAddress(ipaddress.ip_address(ip)))
    except ValueError:
        # Передали домен вместо IP
        san_entries.insert(0, x509.DNSName(ip))

    for h in (extra_hosts or []):
        try:
            san_entries.append(x509.IPAddress(ipaddress.ip_address(h)))
        except ValueError:
            san_entries.append(x509.DNSName(h))

    srv_name = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "RU"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ValueDetector Pro"),
        x509.NameAttribute(NameOID.COMMON_NAME, ip),
    ])

    srv_cert = (
        x509.CertificateBuilder()
        .subject_name(srv_name)
        .issuer_name(ca_name)
        .public_key(srv_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=SERVER_DAYS))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        # ── AKI/SKI обязательны для OpenSSL 3.x (Python 3.13+) ──
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(srv_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(
                ca_key.public_key()
            ),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True, content_commitment=False,
                key_encipherment=True, data_encipherment=False,
                key_agreement=False, key_cert_sign=False, crl_sign=False,
                encipher_only=False, decipher_only=False,
            ), critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )

    _write(CERTS_DIR / "server.key", srv_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    _write(CERTS_DIR / "server.crt", srv_cert.public_bytes(serialization.Encoding.PEM))

    print("[3/3] Готово!")
    print(f"\nФайлы в: {CERTS_DIR}")
    print("\n" + "=" * 64)
    print("ДАЛЬНЕЙШИЕ ШАГИ")
    print("=" * 64)
    print("\n1) На СЕРВЕРЕ — в .env добавить:")
    print(f"     SSL_CERTFILE={CERTS_DIR / 'server.crt'}")
    print(f"     SSL_KEYFILE={CERTS_DIR / 'server.key'}")
    print("\n2) В КЛИЕНТ — скопировать ТОЛЬКО ca.pem:")
    print(f"     {CERTS_DIR / 'ca.pem'}")
    print("     →  PGFrontPatch\\certs\\ca.pem")
    print("\n3) В клиенте config.json (%APPDATA%\\ValueDetectorPro\\config.json):")
    print(f'     "backend_url": "https://{ip}:8000"')
    print(f'     "ws_url":      "wss://{ip}:8000/ws"')
    print("\n4) Перезапустить сервер и клиент.")
    print("\n⚠️  ca.key и server.key НЕ ПЕРЕДАВАТЬ никому.")
    print("=" * 64 + "\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python make_cert.py <IP_или_домен> [доп_хосты...]")
        print("Пример:       python make_cert.py 62.113.115.185")
        sys.exit(1)
    generate(sys.argv[1], sys.argv[2:])
