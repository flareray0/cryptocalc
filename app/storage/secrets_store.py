from __future__ import annotations

import sys
import base64
import ctypes
import json
from ctypes import wintypes
from app.storage.settings import get_paths


class DATA_BLOB(ctypes.Structure):
    _fields_ = [
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte)),
    ]


def _blob_from_bytes(data: bytes) -> DATA_BLOB:
    buf = (ctypes.c_byte * len(data))(*data)
    return DATA_BLOB(len(data), buf)


def _bytes_from_blob(blob: DATA_BLOB) -> bytes:
    return ctypes.string_at(blob.pbData, blob.cbData)


def _dpapi_encrypt(data: bytes) -> bytes:
    if ctypes.windll is None:  # pragma: no cover
        return data
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    in_blob = _blob_from_bytes(data)
    out_blob = DATA_BLOB()
    if not crypt32.CryptProtectData(
        ctypes.byref(in_blob),
        "cryptocalc",
        None,
        None,
        None,
        0,
        ctypes.byref(out_blob),
    ):
        raise OSError("CryptProtectData failed")
    try:
        return _bytes_from_blob(out_blob)
    finally:
        kernel32.LocalFree(out_blob.pbData)


def _dpapi_decrypt(data: bytes) -> bytes:
    if ctypes.windll is None:  # pragma: no cover
        return data
    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    in_blob = _blob_from_bytes(data)
    out_blob = DATA_BLOB()
    if not crypt32.CryptUnprotectData(
        ctypes.byref(in_blob),
        None,
        None,
        None,
        None,
        0,
        ctypes.byref(out_blob),
    ):
        raise OSError("CryptUnprotectData failed")
    try:
        return _bytes_from_blob(out_blob)
    finally:
        kernel32.LocalFree(out_blob.pbData)


def _assert_windows() -> None:
    if sys.platform != "win32":
        raise OSError(
            "SecretsStore は Windows DPAPI 専用です。"
            " Linux / macOS では app/storage/secrets/ 配下のファイルを手動で管理するか、"
            " 環境変数経由で API キーを渡してください。"
        )


class SecretsStore:
    def __init__(self, file_name: str = "binance_japan_api.secrets.json") -> None:
        self.path = get_paths().secrets / file_name

    def save(self, payload: dict[str, str]) -> None:
        _assert_windows()
        encoded = base64.b64encode(
            _dpapi_encrypt(json.dumps(payload).encode("utf-8"))
        ).decode("ascii")
        with self.path.open("w", encoding="utf-8") as fh:
            json.dump({"protected": True, "payload": encoded}, fh, ensure_ascii=False, indent=2)

    def load(self) -> dict[str, str] | None:
        _assert_windows()
        if not self.path.exists():
            return None
        with self.path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        blob = base64.b64decode(raw["payload"])
        return json.loads(_dpapi_decrypt(blob).decode("utf-8"))

    def delete(self) -> None:
        if self.path.exists():
            self.path.unlink()
