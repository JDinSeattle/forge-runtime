def clamp(value, low, high):
    if low > high:
        raise ValueError("inverted bounds")
    return max(low, min(value, high))
