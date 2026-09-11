def chunk_text(text, max_bytes):
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    return [text[i:i + max_bytes] for i in range(0, len(text), max_bytes)]
