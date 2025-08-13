# ETL + Analytics Mini Project (PostgreSQL · Python · Airflow)

Small, production-style ETL pipeline that:
- **Extracts** data from CSV (and is ready for API sources),
- **Transforms**/validates with Python,
- **Loads** into **PostgreSQL** (idempotent upsert),
- Runs **analytics SQL** and **exports CSVs**,
- (Optional) **Orchestrates** via **Apache Airflow**.

> Built to be simple, rerunnable, and resume-ready. Uses `.env` for config and logs row counts/runtime so you can quote metrics.

---

## Table of Contents
- [Stack](#stack)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Quick Start](#quick-start)
- [Loaders](#loaders)
- [Analytics](#analytics)
- [Schema](#schema)
- [Airflow (Optional)](#airflow-optional)
- [Troubleshooting](#troubleshooting)
- [Notes & Next Steps](#notes--next-ste)
