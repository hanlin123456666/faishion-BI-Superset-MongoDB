# FAIshion Analytics Platform

End-to-end analytics pipeline using Docker, MongoDB, MySQL, Python ETL, and Apache Superset.

---

## 1. Architecture Overview

MongoDB (Cloud)
   ↓  Python ETL (incremental / full)
MySQL (Docker, analytics schema)
   ↓
Superset (Docker)
   ↓
ngrok (public access: use ngrok to expose localhost 8088 to internet)

---

## 2. Components

### MongoDB (Cloud)
- Source of truth
- Stores application data:
  - userlogs
  - users
  - test accounts

### ETL (Docker – Python)
- Incremental ETL for `userlogs`
- Full refresh ETL for `users` and `test_acct`
- Writes analytics data into MySQL

### MySQL (Docker)
- Analytics database
- Used only for BI / reporting
- Data persisted via Docker volumes

### Superset (Docker)
- BI tool for visualization
- Connects to MySQL using SQLAlchemy

---

## 3. Containers

| Container | Purpose |
|---------|--------|
| mysql | Analytics database |
| superset | BI & dashboards |
| etl | Data ingestion |

All containers are orchestrated via Docker Compose.

---

## 4. Database Connection

MySQL SQLAlchemy URI:

mysql+pymysql://root:root@mysql:3306/FAIshion

Notes:
- `mysql` is the Docker service name
- Containers communicate over Docker internal network

---

## 5. ETL Strategy

### 5.1 Incremental ETL – userlogs

Incremental load based on timestamp.

State example:

{
  "userlogs_last_timestamp": "2025-12-21T04:57:47.986000",
}

Process:
- Query MongoDB for records newer than last timestamp
- Append new rows into MySQL
- Update state file after successful run
- Use 1970-01-01T00:00:00 in order to do full ETL
---

### 5.2 Full ETL – users / test_acct

Process:
- Truncate table
- Reload full dataset
- Used for small / dimension tables

---

## 6. Startup Flow

### Step 1: Start core services

docker compose up -d
- Creates and starts all services defined in docker-compose.yml
- Runs containers in the background

Starts:
- MySQL
- Superset

---

### Step 2: Verify MySQL driver in Superset

docker compose exec superset /app/.venv/bin/python -c "import MySQLdb; print('MySQLdb OK')"

Expected output:
MySQLdb OK

---

### Step 3: Run ETL container

docker compose up etl

This will:
- Install ETL dependencies
- Run main.py
- Dump data into MySQL container

---

## 7. Superset Setup

1. Open Superset UI:
   http://localhost:8088

2. Login as admin

3. Add database connection:
   - SQLAlchemy URI:
     mysql+pymysql://root:root@mysql:3306/FAIshion

4. Create datasets from MySQL tables
5. Build charts and dashboards

---

## 8. Rebuilding Superset (Important)

If Dockerfile or Superset config changes, rebuild is required.

### Full reset

docker compose down --volumes
docker compose build superset --no-cache
docker compose up -d

Reason:
- Python dependencies are baked into image
- Superset metadata stored in Docker volumes

---

## 9. Start the container
If there is no change, you go into the container and start the services.


## 10. Common Commands

List running containers:
docker ps

Enter Superset container:
docker compose exec superset /bin/sh

Enter MySQL container:
docker compose exec mysql mysql -u root -p

---

## 10. Security Notes

- Docker images do NOT contain data
- Data is stored in Docker volumes
- Sharing images is safe
- Sharing volumes or DB credentials is NOT safe

---

## 11. Summary

- Dockerized analytics platform
- Incremental + full ETL design
- Clear separation of concerns
- Ready for public Superset dashboards

Use cases:
- Internal analytics
- Data engineering demos
- Portfolio projects

---

## 12. Future Improvements

- Scheduled ETL (cron / Airflow)
- Read-only MySQL user for Superset
- ETL CI validation
- Managed MySQL for production
