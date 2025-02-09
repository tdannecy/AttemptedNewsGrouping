"""
utils.py

Contains generic helper functions (hashing, token counting, chunking, etc.)
and the CVE regex from the original code.
"""

import re
import hashlib

MAX_TOKEN_CHUNK = 70000  # from the original script (~70k tokens)
CVE_REGEX = r'\bCVE-\d{4}-\d{4,7}\b'

def generate_content_hash(text):
    """Generate a simple MD5 hash for content."""
    return hashlib.md5(text.encode()).hexdigest()

def approximate_tokens(text: str) -> int:
    """Roughly estimate tokens by counting words and multiplying by ~1.3."""
    return int(len(text.split()) * 1.3)

def chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK):
    """
    Splits article summaries into chunks without exceeding max_token_chunk.
    If a single article alone exceeds max_token_chunk, yield it alone.
    """
    current_chunk = {}
    current_tokens = 0

    for link, summary in summaries_dict.items():
        tokens_for_article = approximate_tokens(summary)
        if tokens_for_article > max_token_chunk:
            # If article alone exceeds chunk limit, yield current then yield it alone
            if current_chunk:
                yield current_chunk
                current_chunk = {}
                current_tokens = 0
            yield {link: summary}
            continue

        if current_tokens + tokens_for_article > max_token_chunk:
            if current_chunk:
                yield current_chunk
            current_chunk = {link: summary}
            current_tokens = tokens_for_article
        else:
            current_chunk[link] = summary
            current_tokens += tokens_for_article

    if current_chunk:
        yield current_chunk

def extract_cves(text: str):
    """Extract a set of unique CVE numbers from the provided text."""
    return set(re.findall(CVE_REGEX, text))
