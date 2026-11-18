# fx-sync-engine
# Open Banking & Multi-Currency Exchange Rate Sync Engine

> For the full project journey, beginner-friendly architecture walkthrough, implementation status, and onboarding guide, see [Project Documentation](docs/PROJECT_DOCUMENTATION.md).

## Project Overview

Cross-border payment platforms rely on accurate foreign exchange (FX) rates to process international transactions. Incorrect exchange rates can lead to financial loss, regulatory issues, and poor customer experience.

This project simulates a production-grade FX Synchronization Engine that ingests exchange rates from multiple providers, validates data quality, processes events through Kafka, caches frequently accessed rates, stores historical data, and exposes data through APIs while maintaining observability through monitoring tools.

---

## Business Problem

Foreign exchange rates are retrieved from multiple providers every minute.

Challenges:

- API failures
- Data quality issues
- Rate anomalies
- Network interruptions
- Schema changes
- Missing currency pairs

The system ensures reliable data ingestion through validation, monitoring, and event-driven processing.

---

## Architecture

```text
External FX APIs
       │
       ▼
   Airflow DAG
       │
       ▼
 Kafka Producer
       │
       ▼
 Kafka Topic
       │
       ▼
 Kafka Consumer
       │
       ├────────► Redis Cache
       │
       ▼
 Great Expectations
       │
       ▼
 PostgreSQL
       │
       ▼
 FastAPI Service
       │
       ▼
 Prometheus Metrics
       │
       ▼
 Grafana Dashboard
```

---

## Technology Stack

| Component | Technology |
|------------|------------|
| Language | Python 3.11 |
| Database | PostgreSQL 16 |
| Scheduler | Apache Airflow |
| Streaming | Apache Kafka |
| Cache | Redis |
| Validation | Great Expectations |
| API | FastAPI |
| Monitoring | Prometheus |
| Observability | Grafana |
| Containerization | Docker |
| Testing | Pytest |
| IDE | VS Code |

---

## Project Structure

```text
fx-sync-engine/

├── configs/
├── dags/
├── docker/
│   ├── airflow/
│   ├── grafana/
│   └── prometheus/
│
├── docs/
│   └── Project_Manual.docx
│
├── great_expectations/
├── logs/
├── sql/
│
├── src/
│   ├── api/
│   ├── cache/
│   ├── clients/
│   ├── consumers/
│   ├── database/
│   ├── monitoring/
│   ├── producers/
│   └── validators/
│
├── tests/
│
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## Features

### Exchange Rate Ingestion

- Pull rates from external providers
- Retry failed requests
- Handle API outages

### Event Streaming

- Kafka Producer publishes rates
- Kafka Consumer processes rates

### Data Validation

- Missing values detection
- Exchange rate range validation
- Currency integrity checks
- Schema validation

### Data Storage

- Historical exchange rate storage
- Audit logging
- Metadata tracking

### Caching

- Frequently requested rates stored in Redis
- Reduced database load

### Monitoring

- Application metrics
- Kafka metrics
- API health metrics
- Database metrics

### Dashboarding

- Grafana dashboards
- Pipeline monitoring
- Exchange rate visualization

---

## Future Improvements

- Multi-provider failover strategy
- Airflow scheduling
- CI/CD using GitHub Actions
- Alerting system
- TimescaleDB migration
- Kubernetes deployment

---

## Learning Outcomes

- Event-driven architecture
- Data engineering workflows
- Financial data processing
- Distributed systems
- Data validation
- Infrastructure monitoring
- Docker containerization