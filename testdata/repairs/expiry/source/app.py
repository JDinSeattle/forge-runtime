def is_expired(created_at, ttl_seconds, now):
    if ttl_seconds < 0:
        raise ValueError("negative ttl")
    return now > created_at + ttl_seconds
