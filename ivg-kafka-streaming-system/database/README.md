# IVG Gallery Event Streaming System (Apache Kafka)

## Pregled projekta

Ovaj projekat implementira **event-driven streaming sistem** za virtuelnu umjetničku galeriju (IVG), korišćenjem tehnologija **Apache Kafka, PostgreSQL i Streamlit**.

Sistem simulira korisničke interakcije u realnom vremenu unutar digitalne galerije, obrađuje događaje kroz **stream processing arhitekturu**, čuva ih u bazi podataka i omogućava analitiku kroz dashboard.

Projekat demonstrira ključne principe event-driven arhitekture:

- obrada podataka u realnom vremenu (real-time stream processing)
- message broker sistem (Apache Kafka)
- višestruki consumers za različite poslovne svrhe
- routing i filtriranje događaja
- otpornost sistema na greške (Dead Letter Queue)
- perzistentni sloj podataka (PostgreSQL)
- analitički dashboard (Streamlit)

# Domen problema

Sistem modeluje virtuelnu umjetničku galeriju u kojoj korisnici interaguju sa umjetničkim djelima kroz:

- pregled umjetničkih djela
- dodavanje u omiljene
- pretragu umjetnosti
- klikove na preporuke
- kreiranje novih umjetničkih djela

Svaka interakcija se modeluje kao **event** i šalje u Kafka streaming sistem u realnom vremenu.

# Model događaja

Svi događaji koriste jedinstvenu JSON strukturu:

```json
{
  "event_id": "uuid",
  "event_type": "string",
  "timestamp": "ISO-8601",
  "user_id": "string",
  "payload": {}
}
```

# Tipovi događaja

- `artwork_viewed`
- `artwork_favorited`
- `search_performed`
- `recommendation_clicked`
- `artwork_created`

# Arhitektura sistema

Sistem se sastoji od sljedećih komponenti:

- **Producer** → generiše simulirane korisničke događaje

- **Kafka topic `ivg.events`** → glavni event stream

- **Consumers**:
  - Analytics consumer (real-time agregacije)
  - Reporting consumer (perzistencija u PostgreSQL)
  - Validation consumer (validacija i routing događaja)

- **Kafka topic `ivg.valid`** → validni događaji (filtered stream)

- **Kafka topic `ivg.dlq`** → nevalidni događaji (Dead Letter Queue)

- **PostgreSQL baza** → trajno skladištenje podataka

- **Streamlit dashboard** → vizualizacija analitike

# Kafka dizajn

## Topic: `ivg.events`

- glavni ulazni stream događaja
- particionisanje po `user_id`
- omogućava očuvanje redosljeda događaja po korisniku

## Topic: `ivg.valid`

- filtrirani, validirani događaji
- koristi se za dalju obradu i analitiku

## Topic: `ivg.dlq`

- skladišti nevalidne ili oštećene poruke
- omogućava debugging i monitoring

# Producer

Producer simulira realno ponašanje korisnika i kontinuirano generiše događaje.

Ključne funkcionalnosti:

- generisanje nasumičnih događaja
- simulacija realnih user interakcija
- definisani tipovi umjetničkih djela
- routing po ključu `user_id` (partitioning strategy)

# Consumers

## 1. Analytics Consumer

- real-time obrada događaja
- računa:
  - popularnost umjetničkih djela
  - frekvenciju tagova
  - engagement score

## 2. Reporting Consumer

- perzistira događaje u PostgreSQL bazu
- obezbjeđuje idempotentnost upisa kroz `event_id`
- omogućava analitičke upite nad istorijskim podacima

## 3. Validation Consumer

- validira strukturu dolaznih događaja
- provjerava obavezna polja i JSON strukturu
- implementira **routing logiku**:
  - validni događaji → `ivg.valid`
  - nevalidni događaji → `ivg.dlq`

# Dead Letter Queue (DLQ)

Nevalidni događaji se preusmjeravaju u `ivg.dlq` u slučajevima:

- nedostajućih obaveznih polja
- nevalidnog JSON formata
- oštećenih ili nepotpunih poruka

DLQ omogućava stabilnost sistema i izolaciju grešaka bez prekida stream-a.

# Šema baze podataka

```sql
CREATE TABLE ivg_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT,
    user_id TEXT,
    timestamp TIMESTAMPTZ,
    payload JSONB
);
```

# Dashboard (Streamlit)

Dashboard omogućava real-time analitiku nad Kafka → PostgreSQL pipeline-om:

- ukupan broj događaja
- distribucija tipova događaja
- najpregledanija umjetnička djela
- najaktivniji korisnici
- posljednji događaji

# Pokretanje projekta

## 1. Start infrastrukture

```bash
docker compose up -d
```

## 2. Instalacija zavisnosti

```bash
pip install -r requirements.txt
```

## 3. Pokretanje producer-a

```bash
python producer.py
```

## 4. Pokretanje consumers-a

```bash
python analytics_consumer.py
python reporting_consumer.py
python validation_consumer.py
```

## 5. Pokretanje dashboard-a

```bash
streamlit run dashboard.py
```

# Korišćene tehnologije

- Apache Kafka
- Python
- PostgreSQL
- Streamlit
- Docker
- psycopg2
- confluent-kafka

# Ključni koncepti demonstrirani u projektu

- Event-driven arhitektura
- Stream processing
- Message broker sistem
- Consumer groups
- Partitioning strategy (key-based routing)
- Data validation pipeline
- Dead Letter Queue (DLQ pattern)
- Data persistence layer
- Real-time analytics
