# Eratani Data Engineer Technical Assessment

Pipeline data end-to-end untuk memproses data produksi pertanian menggunakan **Apache Airflow**, **dbt**, dan **PostgreSQL**.

Project ini dibuat sebagai bagian dari **Eratani Data Engineer Technical Assessment**.

---

## 🔧 Tech Stack

- Apache Airflow — Orkestrasi pipeline
- dbt — Transformasi & data modeling
- PostgreSQL — Data warehouse
- Docker & Docker Compose — Containerization
- Python — Data ingestion

---

## 📂 Project Structure

```text
eratani_etl/
├── dags/
│   └── eratani_agriculture_dag.py
├── dbt_projects/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   └── stg_agriculture.sql
│       └── marts/
│           ├── fact_farm_production.sql
│           ├── agriculture_metrics.sql
│           └── agriculture_metrics_daily.sql
├── data/
│   └── agriculture_dataset.csv
├── docker-compose.yml
├── .gitignore
└── README.md
```
# Alur Pipeline Data
1. Ingestion (Airflow)

- File agriculture_dataset.csv dibaca menggunakan Python

- Data dimasukkan ke tabel staging_agriculture di PostgreSQL

- Proses ingestion bersifat idempotent (TRUNCATE sebelum insert)

- Jumlah baris data dicatat di log Airflow

2. Transformasi (dbt)

- stg_agriculture
- Membersihkan data (casting tipe data, trimming string, filter null)

- fact_farm_production
- Tabel fakta berisi data pertanian yang sudah bersih dan terstruktur

- agriculture_metrics_daily
- Tabel metrics yang berisi insight siap pakai

3. Orcrestration (penjadwalan)

- Airflow menjalankan ingestion dan dbt secara berurutan

- DAG dijalankan setiap hari pukul 06:00 UTC

# Cara Menjalankan Project
1. Jalankan Docker Compose
```
docker compose up -d

```
2. Akses Airflow UI
```
URL: http://localhost:8080

Username: admin

Password: admin123
```
3. Jalankan DAG

Aktifkan dan jalankan DAG:
```
eratani_agriculture_etl
```
Pipeline akan:

Load CSV ke PostgreSQL

Menjalankan seluruh model dbt

Menghasilkan tabel metrics

# 📝 Notes

Project ini dijalankan sepenuhnya secara lokal menggunakan Docker

Struktur pipeline mengikuti best practice data engineering:

Orkestrasi terpisah dari transformasi

Transformasi terpusat di dbt

Metrics siap pakai untuk analisis atau BI tools
