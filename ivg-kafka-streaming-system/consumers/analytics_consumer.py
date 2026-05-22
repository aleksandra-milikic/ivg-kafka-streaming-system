from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    "ivg.events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="analytics-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

artwork_views = defaultdict(int)
artwork_likes = defaultdict(int)
tag_counter = defaultdict(int)
engagement_score = defaultdict(int)


def process_event(event):
    if not isinstance(event, dict):
        print("Skipping invalid event format")
        return

    event_type = event.get("event_type")
    payload = event.get("payload")

    if not isinstance(payload, dict):
        payload = {}

    if event_type == "artwork_viewed":
        art_id = payload.get("artwork_id")
        if art_id:
            artwork_views[art_id] += 1

            engagement = payload.get("engagement_level", "low")

            score_map = {
                "low": 1,
                "medium": 2,
                "high": 3
            }

            engagement_score[art_id] += score_map.get(engagement, 1)

    elif event_type == "artwork_favorited":
        art_id = payload.get("artwork_id")
        if art_id:
            artwork_likes[art_id] += 1
            engagement_score[art_id] += 5

    tags = payload.get("tags")
    if isinstance(tags, list):
        for tag in tags:
            tag_counter[tag] += 1


def print_stats():
    print("\n==============================")
    print(" REAL-TIME ANALYTICS REPORT")
    print("==============================")

    print("\nTOP ARTWORKS (VIEWS):")
    for art, views in sorted(artwork_views.items(), key=lambda x: x[1], reverse=True):
        print(f"- {art}: {views} views")

    print("\nTOP ARTWORKS (LIKES):")
    for art, likes in sorted(artwork_likes.items(), key=lambda x: x[1], reverse=True):
        print(f"- {art}: {likes} likes")

    print("\nTOP TAGS:")
    for tag, count in sorted(tag_counter.items(), key=lambda x: x[1], reverse=True):
        print(f"- {tag}: {count}")

    print("\nENGAGEMENT SCORE:")
    for art, score in sorted(engagement_score.items(), key=lambda x: x[1], reverse=True):
        print(f"- {art}: {score}")


print("Analytics consumer started...")

counter = 0

for message in consumer:
    try:
        process_event(message.value)

        counter += 1

        if counter % 5 == 0:
            print_stats()

    except Exception as e:
        print(f"[ERROR] Analytics consumer failed: {e}")