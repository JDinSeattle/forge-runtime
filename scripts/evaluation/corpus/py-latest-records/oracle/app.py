def latest_by_id(events):
    seen, result = set(), []
    for event in reversed(events):
        if event["id"] not in seen:
            seen.add(event["id"])
            result.append(dict(event))
    return list(reversed(result))
