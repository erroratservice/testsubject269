from urllib.parse import urlparse, parse_qs
from base64 import b32decode, b16encode
from typing import Optional
import logging

try:
    # Optional dependency used to compute magnet from .torrent
    from torrentool.api import Torrent
except Exception:
    Torrent = None


def _normalize_btih(value: str) -> Optional[str]:
    """
    Normalize BTIH to 40-char lowercase hex.
    Accepts:
      - 'urn:btih:...' or raw
      - 40-char hex
      - 32-char base32 -> convert to hex
    """
    if not value:
        return None
    if value.startswith("urn:btih:"):
        value = value[len("urn:btih:") :]

    s = value.strip()

    # 40-char hex
    if len(s) == 40 and all(c in "0123456789abcdefABCDEF" for c in s):
        return s.lower()

    # 32-char base32 -> hex
    if len(s) == 32:
        try:
            return b16encode(b32decode(s.upper())).decode().lower()
        except Exception as e:
            logging.error(f"Failed to decode BTIH base32: {e}")
            return None

    return None


def get_hash_magnet(magnet: str) -> Optional[str]:
    """
    Extract and normalize BTIH from a magnet link to 40-char hex.
    Returns None if invalid.
    """
    if not magnet or not magnet.startswith("magnet:"):
        logging.error("Invalid magnet URI: missing 'magnet:' scheme.")
        return None

    parsed = urlparse(magnet)
    qs = parse_qs(parsed.query)
    xts = qs.get("xt", [])

    for xt in xts:
        if isinstance(xt, str) and xt.startswith("urn:btih:"):
            h = _normalize_btih(xt)
            if h:
                return h

    logging.error('Invalid magnet URI: no valid "xt=urn:btih:..." parameter.')
    return None


def get_hash_file(path: str) -> Optional[str]:
    """
    Compute v1 info-hash from a .torrent file (40-char hex).
    """
    if not Torrent:
        logging.error("torrentool is not available; install 'torrentool'.")
        return None

    try:
        tr = Torrent.from_file(path)
        return get_hash_magnet(tr.magnet_link)
    except Exception as e:
        logging.error(f"Failed to parse .torrent '{path}': {e}")
        return None
      
