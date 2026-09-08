def clamp(value, low, high):
    if low > high:
        raise ValueError("inverted bounds")
    return min(low, max(value, high))
