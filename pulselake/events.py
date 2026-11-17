"""Deterministic, synthetic events. No real listener information is used."""
import random
import uuid
from datetime import datetime, timedelta, timezone

SONGS = [
    ("s001", "Neon Rain", "Paper Satellites", 213),
    ("s002", "Morning Train", "The Daylights", 184),
    ("s003", "Quiet Orbit", "Lunar Arcade", 247),
    ("s004", "City Echoes", "Paper Satellites", 198),
    ("s005", "Ocean Glass", "The Daylights", 226),
]


def generate_events(count=1000, seed=42, run_id="demo", start=None,
                    duplicate_every=20, invalid_every=50, late_every=30):
    """Yield count deliveries. Duplicates are exact copies, with the same event_id."""
    rng = random.Random(seed)
    start = start or datetime.now(timezone.utc)
    if start.tzinfo is None:
        raise ValueError("start must include a timezone")
    previous = None
    for index in range(count):
        if duplicate_every and index and index % duplicate_every == 0:
            yield dict(previous)
            continue
        song_id, title, artist, duration = rng.choice(SONGS)
        event_type = rng.choices(["listen", "page_view", "login"], [8, 1, 1])[0]
        timestamp = start + timedelta(seconds=index)
        if late_every and index and index % late_every == 0:
            timestamp -= timedelta(hours=2)
        event = {
            "schema_version": 1,
            "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"pulselake:{run_id}:{index}")),
            "event_time": timestamp.isoformat(),
            "event_type": event_type,
            "user_id": f"u{rng.randint(1, 100):04d}",
            "session_id": f"{run_id}-{index // 12}",
            "song_id": song_id if event_type == "listen" else None,
            "song_title": title if event_type == "listen" else None,
            "artist": artist if event_type == "listen" else None,
            "duration_seconds": duration if event_type == "listen" else 0,
            "country": rng.choice(["IN", "US", "GB", "DE"]),
            "subscription": rng.choice(["free", "premium"]),
        }
        if invalid_every and index and index % invalid_every == 0:
            event["user_id"] = None
        previous = dict(event)
        yield event
