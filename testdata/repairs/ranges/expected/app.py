def merge_ranges(ranges):
    result = []
    for start, end in sorted(ranges):
        if start > end:
            raise ValueError("inverted range")
        if result and start <= result[-1][1]:
            result[-1][1] = max(result[-1][1], end)
        else:
            result.append([start, end])
    return result
