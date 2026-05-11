
---

### airflow-docker

```markdown
# Airflow ML Pipeline: Car Price Prediction

Automating model training and inference with Apache Airflow.

## What's Inside

- **pipeline** – loads data, preprocesses, trains several classifiers (LogisticRegression, RandomForest, SVC), selects the best one via cross‑validation, and saves it.
- **predict** – loads the saved model and predicts on JSON files from `data/test`.
- The DAG is scheduled to run **daily at 15:00**.

## Stack

Python · Airflow 3.2.1 · Docker Compose · Scikit-learn · Pandas · Dill · PostgreSQL · Redis

## Quick Start

1. **Clone the repository:**
   ```bash
   git clone <URL>
   cd airflow-docker

2. **Set up environment variables:**

```bash
cp .env.example .env
# edit .env and insert your real keys

3. **Start the services:**

```bash
docker compose up -d

4. **Open Airflow web UI:**
Go to http://localhost:8080
Login: airflow
Password: airflow

5. **Enable the DAG:**
Find car_price_prediction in the list, toggle it on, and trigger manually or wait for the schedule.
