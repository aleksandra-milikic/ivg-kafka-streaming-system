# IVG Gallery Event Streaming System – Apache Kafka

## Project Overview

This project implements an **event-driven streaming system** for a virtual art gallery (IVG), using **Apache Kafka, Python, PostgreSQL, Docker, and Streamlit**.

The system simulates user interactions within a digital art gallery, generates events in real time, processes them through multiple Kafka consumers, stores event data in a PostgreSQL database, and provides analytical insights through a Streamlit dashboard.

The project demonstrates key concepts of event-driven architecture and stream processing, including:

- Real-time event generation and processing.
- Message broker architecture using Apache Kafka.
- Multiple consumers for different processing tasks.
- Event validation and routing.
- Dead Letter Queue (DLQ) pattern for invalid events.
- Persistent data storage using PostgreSQL.
- Data analysis and visualization through Streamlit.

## Problem Domain

The system models a virtual art gallery where users interact with artworks through different activities:

- Viewing artworks.
- Adding artworks to favorites.
- Searching for artworks.
- Clicking on recommendations.
- Creating new artworks.

Each user activity is represented as an event and published to the Kafka streaming system.

## Event Model

All generated events follow a common JSON structure:

```json
{
  "event_id": "uuid",
  "event_type": "string",
  "timestamp": "ISO-8601",
  "user_id": "string",
  "payload": {}
}
```

The event contains:

- `event_id` – unique identifier generated for each event.
- `event_type` – type of user activity.
- `timestamp` – event creation time in UTC.
- `user_id` – identifier of the user who generated the event.
- `payload` – additional information related to the event.

The `payload` field contains event-specific data.

## Event Types

The producer generates the following event types:

- `artwork_viewed`
- `artwork_favorited`
- `search_performed`
- `recommendation_clicked`
- `artwork_created`

Events are generated using randomly selected users, artworks, and event-specific data.

## System Architecture

The system consists of the following components:

- **Producer** – generates simulated user activity events.
- **Kafka topic `ivg.events`** – main input stream for generated events.
- **Validation Consumer** – checks event structure and routes valid and invalid events.
- **Kafka topic `ivg.valid`** – receives events that pass validation.
- **Kafka topic `ivg.dlq`** – receives invalid events (Dead Letter Queue).
- **Analytics Consumer** – performs in-memory event aggregation and prints analytical reports.
- **Reporting Consumer** – stores event records in PostgreSQL.
- **PostgreSQL** – provides persistent event storage.
- **Streamlit Dashboard** – displays analytical information based on stored database records.

### Architecture Flow

The main event flow starts with the producer publishing events to `ivg.events`.

The validation consumer independently reads from this topic and forwards events to either `ivg.valid` or `ivg.dlq`.

The analytics and reporting consumers also independently read from `ivg.events`:
- Analytics processes events and calculates statistics.
- Reporting stores event records in PostgreSQL.

The Streamlit dashboard retrieves information from PostgreSQL and displays it through tables, metrics, and charts.

**Note:** The validation consumer currently operates as a separate validation and routing pipeline. The analytics and reporting consumers read directly from `ivg.events`, rather than from `ivg.valid`.

## Kafka Design

### Topic: `ivg.events`

- Main input stream for generated events.
- Receives events from the producer.
- Used independently by the validation, analytics, and reporting consumers.
- Messages are published using `user_id` as the Kafka message key.

Using the user ID as a message key allows Kafka to assign messages to partitions based on that key, supporting key-based partitioning and ordering within a partition.

### Topic: `ivg.valid`

- Receives events that pass validation.
- Represents the validated event stream.
- Can be used as an input for additional downstream processing.

### Topic: `ivg.dlq`

- Receives events that fail validation.
- Stores the rejection reason and original event data.
- Helps isolate invalid messages for debugging and further inspection.

## Producer

The producer simulates user behavior in the virtual art gallery and continuously generates events.

Main functionalities:

- Random event generation.
- Simulation of different user interactions.
- Predefined users and artworks.
- Generation of event-specific payloads.
- Unique event ID generation using UUID.
- UTC timestamp generation.
- Message publishing to Kafka topic `ivg.events`.
- Key-based message routing using `user_id`.

The producer also occasionally generates invalid events by removing the `user_id` field, allowing the validation component to detect missing required information.

## Consumers

The system uses three independent Kafka consumers, each responsible for a different processing task.

### 1. Analytics Consumer

The analytics consumer performs real-time in-memory event processing.

It calculates:

- **Artwork views** – number of views per artwork.
- **Artwork likes** – number of favorite actions per artwork.
- **Tag frequency** – frequency of artwork tags.
- **Engagement score** – weighted engagement score based on user interactions.

The engagement score is calculated using the following weights:

| Interaction | Score |
|---|---:|
| Low engagement view | 1 |
| Medium engagement view | 2 |
| High engagement view | 3 |
| Favorite action | 5 |

The consumer prints an analytical report after every five processed events.

The calculated statistics are maintained in memory and displayed in the terminal.

### 2. Reporting Consumer

The reporting consumer is responsible for storing event data in PostgreSQL.

Main functionalities:

- Consumes events from `ivg.events`.
- Extracts event identifiers, types, user IDs, timestamps, and payloads.
- Stores event data in the `ivg_events` table.
- Uses `event_id` as a unique identifier.
- Implements duplicate protection through PostgreSQL's `ON CONFLICT DO NOTHING` statement.
- Attempts to reconnect to the database if a database operation fails.

This provides persistent storage for historical event analysis and dashboard queries.

### 3. Validation Consumer

The validation consumer checks incoming events and routes them according to their validity.

It verifies the presence of the following required fields:

- `event_id`
- `event_type`
- `timestamp`
- `user_id`
- `payload`

It also checks whether the event is a dictionary and whether the `payload` field is a dictionary.

The consumer implements the following routing logic:

- **Valid events** → forwarded to `ivg.valid`.
- **Invalid events** → forwarded to `ivg.dlq`.

Invalid events are accompanied by a rejection reason and the original event data.

This component demonstrates basic event validation and separation of valid and invalid messages.

## Dead Letter Queue (DLQ)

The system uses a Dead Letter Queue pattern to isolate events that do not satisfy the validation requirements.

Events are routed to `ivg.dlq` when required fields are missing or have invalid values, or when the event structure does not satisfy the validation rules.

Each invalid message contains:

- `reason` – explanation of the validation failure.
- `raw_event` – original event data.

The DLQ helps isolate invalid messages without stopping the validation consumer's normal processing loop.

## Database Schema

PostgreSQL is used as the persistent storage layer for event records.

The database schema is defined in `database/schema.sql`.

### Table: `ivg_events`

Stores successfully inserted event records.

```sql
CREATE TABLE IF NOT EXISTS ivg_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB
);
```

### Table: `dlq_events`

The schema also defines a table for storing invalid events:

```sql
CREATE TABLE IF NOT EXISTS dlq_events (
    id SERIAL PRIMARY KEY,
    reason TEXT,
    raw_event JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

The current validation consumer sends invalid events to the Kafka topic `ivg.dlq`. The provided consumer code does not currently insert those events into the PostgreSQL `dlq_events` table.

## Dashboard – Streamlit

The project includes a Streamlit dashboard for visualizing event data stored in PostgreSQL.

The dashboard provides:

- **Total Events** – total number of records in the database.
- **Event Type Distribution** – number of events grouped by event type.
- **Most Viewed Artworks** – artworks ranked by recorded view events.
- **Most Active Users** – users ranked by their number of recorded events.
- **Latest Events** – the most recently stored event records.

The dashboard uses SQL queries to retrieve data from PostgreSQL and displays the results through metrics, tables, and charts.

Database query results are cached for 10 seconds.

## Project Structure

```text
IVG-Kafka-Streaming-System/
│
├── README.md
├── docker-compose.yml
├── config.py
├── dashboard.py
├── producer.py
├── requirements.txt
├── run_all.py
│
├── consumers/
│   ├── analytics_consumer.py
│   ├── reporting_consumer.py
│   └── validation_consumer.py
│
└── database/
    ├── docker-compose.yml
    └── schema.sql
```

## Running the Project

### 1. Start the Infrastructure

The project contains two Docker Compose configurations.

The main `docker-compose.yml` defines the Zookeeper, Kafka, and PostgreSQL services.

The `database/docker-compose.yml` configuration additionally mounts `schema.sql` into the PostgreSQL initialization directory and includes separate internal and external Kafka listeners.

For the database schema initialization and the full Kafka listener configuration, run the following command from the project root:

```bash
docker compose -f database/docker-compose.yml up -d
```

This starts:
- Zookeeper
- Kafka
- PostgreSQL

The database tables are initialized automatically when PostgreSQL creates a new database volume.

### 2. Install Python Dependencies

From the project root, run:

```bash
pip install -r requirements.txt
```

### 3. Start the Producer

```bash
python producer.py
```

The producer continuously generates events and publishes them to `ivg.events`.

### 4. Start the Consumers

Open separate terminal windows and run:

**Analytics Consumer:**

```bash
python consumers/analytics_consumer.py
```

**Reporting Consumer:**

```bash
python consumers/reporting_consumer.py
```

**Validation Consumer:**

```bash
python consumers/validation_consumer.py
```

Each consumer runs independently and processes events from the Kafka topic `ivg.events`.

### 5. Start the Dashboard

```bash
streamlit run dashboard.py
```

The dashboard will be available through the local Streamlit address displayed in the terminal.

### Alternative: Run the Components Together

The project also includes a `run_all.py` script that starts the producer, all three consumers, and the Streamlit dashboard as separate processes.

Run it from the project root:

```bash
python run_all.py
```

Press `Ctrl + C` to initiate shutdown of the started processes.

## Configuration

The project includes a `config.py` file that reads configuration values from environment variables using `python-dotenv`.

Supported variables include:

- `KAFKA_BOOTSTRAP`
- `POSTGRES_HOST`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_PORT`

The current producer, consumers, and dashboard also contain connection settings directly in their source files. Their connection credentials must match the running Docker services.

## Technologies Used

- **Apache Kafka** – event streaming and message broker.
- **Python** – event generation and processing.
- **PostgreSQL** – persistent event storage.
- **Streamlit** – analytical dashboard and visualization.
- **Docker Compose** – infrastructure setup and service orchestration.
- **psycopg2** – PostgreSQL connectivity.
- **pandas** – data retrieval and manipulation.
- **kafka-python** – Kafka producer and consumer implementation.

## Key Concepts Demonstrated

- Event-driven architecture.
- Real-time event generation and processing.
- Message broker architecture.
- Kafka topics and consumer groups.
- Key-based message partitioning.
- Multiple independent consumers.
- Event validation and routing.
- Dead Letter Queue (DLQ) pattern.
- Data persistence using PostgreSQL.
- SQL-based analytical queries.
- Data visualization using Streamlit.
- Containerized infrastructure with Docker Compose.
