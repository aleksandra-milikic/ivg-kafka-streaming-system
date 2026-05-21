from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    'ivg.events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='analytics-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


artwork_views = defaultdict(int)
tag_counter = defaultdict(int)
engagement_score = defaultdict(int)


def process_event(event):
    event_type = event["event_type"]
    payload = event.get("payload", {})

    
    if event_type == "artwork_viewed":
        art_id = payload.get("artwork_id")
        artwork_views[art_id] += 1

        
        engagement = payload.get("engagement_level", "low")

        if engagement == "high":
            engagement_score[art_id] += 3
        elif engagement == "medium":
            engagement_score[art_id] += 2
        else:
            engagement_score[art_id] += 1

    
    if event_type == "artwork_favorited":
        art_id = payload.get("artwork_id")
        engagement_score[art_id] += 5

    
    tags = payload.get("tags", [])
    for tag in tags:
        tag_counter[tag] += 1


def print_stats():
    print("\nREAL-TIME ANALYTICS")

    print("\nTOP ARTWORKS:")
    for art, views in sorted(artwork_views.items(), key=lambda x: x[1], reverse=True):
        print(f"{art}: {views} views")

    print("\nTOP TAGS:")
    for tag, count in sorted(tag_counter.items(), key=lambda x: x[1], reverse=True):
        print(f"{tag}: {count}")

    print("\nENGAGEMENT SCORE:")
    for art, score in sorted(engagement_score.items(), key=lambda x: x[1], reverse=True):
        print(f"{art}: {score}")


print("Analytics consumer started...")

counter = 0

for message in consumer:
    event = message.value

    process_event(event)

    counter += 1

    
    if counter % 5 == 0:
        print_stats()