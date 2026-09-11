def chunk_text(text, max_bytes):
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    result, chunk, used = [], [], 0
    for char in text:
        width = len(char.encode("utf-8"))
        if width > max_bytes:
            raise ValueError("one code point exceeds max_bytes")
        if used + width > max_bytes:
            result.append("".join(chunk))
            chunk, used = [], 0
        chunk.append(char)
        used += width
    if chunk:
        result.append("".join(chunk))
    return result
