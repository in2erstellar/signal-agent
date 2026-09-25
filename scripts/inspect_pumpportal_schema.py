"""Print one live PumpPortal subscribeNewToken message and exit.

The API key comes from PUMPPORTAL_API_KEY. When it is unset the script
connects to the same host without a key so a free creation stream can
still be inspected. Nothing is parsed.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import struct
import sys

HOST = "pumpportal.fun"
PATH_WITH_KEY = "/api/data?api-key={key}"
PATH_WITHOUT_KEY = "/api/data"
TIMEOUT_SECONDS = 45


def _connect(path: str) -> ssl.SSLSocket:
    raw = socket.create_connection((HOST, 443), timeout=TIMEOUT_SECONDS)
    context = ssl.create_default_context()
    sock = context.wrap_socket(raw, server_hostname=HOST)
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {HOST}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    sock.sendall(request.encode())
    buffer = b""
    while b"\r\n\r\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError(f"connection closed during handshake: {buffer!r}")
        buffer += chunk
    head, _rest = buffer.split(b"\r\n\r\n", 1)
    status = head.split(b"\r\n", 1)[0].decode()
    if " 101 " not in status:
        raise RuntimeError(f"websocket upgrade failed: {status} {head.decode(errors='replace')}")
    return sock


def _send_text(sock: ssl.SSLSocket, text: str) -> None:
    payload = text.encode()
    mask = os.urandom(4)
    header = bytearray([0x81])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    elif length < 65536:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", length))
    header.extend(mask)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes(header) + masked)


def _read_text(sock: ssl.SSLSocket) -> str:
    sock.settimeout(TIMEOUT_SECONDS)
    while True:
        frame = _read_frame(sock)
        opcode, payload = frame
        if opcode == 0x1:
            return payload.decode()
        if opcode == 0x8:
            raise RuntimeError(f"server closed the socket: {payload!r}")
        if opcode == 0x9:
            _send_pong(sock, payload)


def _read_exact(sock: ssl.SSLSocket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        part = sock.recv(size - len(chunks))
        if not part:
            raise RuntimeError("socket closed while reading a frame")
        chunks.extend(part)
    return bytes(chunks)


def _read_frame(sock: ssl.SSLSocket) -> tuple[int, bytes]:
    header = _read_exact(sock, 2)
    opcode = header[0] & 0x0F
    length = header[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", _read_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _read_exact(sock, 8))[0]
    if header[1] & 0x80:
        mask = _read_exact(sock, 4)
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(_read_exact(sock, length)))
    else:
        payload = _read_exact(sock, length)
    return opcode, payload


def _send_pong(sock: ssl.SSLSocket, payload: bytes) -> None:
    mask = os.urandom(4)
    frame = bytearray([0x8A, 0x80 | len(payload)])
    frame.extend(mask)
    frame.extend(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes(frame))


def main() -> None:
    api_key = os.environ.get("PUMPPORTAL_API_KEY", "").strip()
    path = PATH_WITH_KEY.format(key=api_key) if api_key else PATH_WITHOUT_KEY
    print(f"connecting path={path.split('api-key=')[0]}", file=sys.stderr)
    sock = _connect(path)
    try:
        _send_text(sock, json.dumps({"method": "subscribeNewToken"}))
        # The first frame is the subscription acknowledgement. Keep reading
        # until a creation payload arrives so the field names are observed,
        # not guessed. Cap the wait at a handful of frames.
        for _ in range(20):
            message = _read_text(sock)
            print(message)
            if "mint" in message:
                break
    finally:
        sock.close()


if __name__ == "__main__":
    main()
