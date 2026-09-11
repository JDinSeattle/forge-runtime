def latest_by_id(events):
    latest = {event["id"]: dict(event) for event in events}
    return list(latest.values())
