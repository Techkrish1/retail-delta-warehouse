# retail-delta-warehouse

A hands-on data engineering project built on **Databricks + Delta Lake**, using a retail company's customer data as the domain. The goal is to implement real warehouse patterns the way they're actually done in production — not toy examples, but the kind of code you'd write on the job and defend in a technical interview.

---

## Problem

Customer data changes. People move, update email addresses, switch regions. A simple `UPDATE` destroys that history, and the business loses the ability to answer questions like:

- *What was this customer's address when they placed that order in 2023?*
- *How many customers lived in California last year vs. this year?*
- *Which customers changed regions after the Q3 marketing push?*

This project solves that with **Slowly Changing Dimension Type 2 (SCD2)** — a data warehousing pattern where every change creates a new versioned row instead of overwriting the old one.

---

## Architecture

```
┌─────────────────────┐
│   Source CRM System │
│  (customer records) │
└──────────┬──────────┘
           │ daily batch / CDC feed
           ▼
┌─────────────────────┐
│   Staging Table     │  ← raw incoming changes land here first
│  (staging_updates)  │
└──────────┬──────────┘
           │
           ▼
┌──────────────────────────────────────────┐
│           SCD2 MERGE Logic               │
│                                          │
│  Pass 1 — Expire changed records         │
│    MERGE: find active rows where data    │
│    changed → set eff_end_date, is_current│
│                                          │
│  Pass 2 — Insert new active versions     │
│    Write fresh rows for all changed +    │
│    new customers as current              │
└──────────┬───────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────┐
│         retail.customer_dim             │
│         (Delta Lake table)              │
│                                         │
│  surrogate_key │ customer_id │ ...      │
│  eff_start_date │ eff_end_date          │
│  is_current │ created_at │ updated_at  │
└──────────┬──────────────────────────────┘
           │
     ┌─────┴──────┐
     ▼            ▼
┌─────────┐  ┌──────────────────┐
│BI / SQL │  │ Point-in-time    │
│ Reports │  │ Historical Query │
│(current │  │ (any past date)  │
│ records)│  └──────────────────┘
└─────────┘
```

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Compute | Databricks (Community / AWS) |
| Storage format | Delta Lake |
| Language | PySpark + Spark SQL |
| Version control | Git + GitHub |

---

## What's Implemented

### `notebooks/scd_type2_customer_dim.py`

Full SCD Type 2 pipeline for the customer dimension table.

**Covers:**
- Initial full load with surrogate key assignment
- Two-pass MERGE strategy (expire → insert) with explanation of why a single MERGE isn't enough
- 5 real-world change scenarios: address change, email change, multi-field change, new customer, no-change pass-through
- Point-in-time historical query
- Data quality assertions post-load
- Delta Lake time travel for audit history

**Table design decisions explained inline:**
- Why `9999-12-31` instead of NULL for open end dates
- Why `is_current` exists alongside `eff_end_date`
- Why the end date is set to `current_date - 1`
- Why the MERGE needs two passes, not one

---

## Running the Notebook

1. Open your Databricks workspace
2. Go to **Workspace → /retail-delta-warehouse → scd_type2_customer_dim**  
   *(or import `notebooks/scd_type2_customer_dim.py` manually via Workspace → Import)*
3. Attach a running cluster (DBR 11.x+ recommended for Delta Lake features)
4. Run all cells top to bottom — each section has markdown explaining what's happening and why

No external dependencies or datasets required. All data is generated inline.

---

## Project Structure

```
retail-delta-warehouse/
│
└── notebooks/
    └── scd_type2_customer_dim.py   ← SCD2 pipeline (current)
```

More patterns will be added here over time — fact table loading, partitioning strategies, incremental processing, etc.

---

## Things This Doesn't Cover (yet)

- Late-arriving data — changes that should have been effective days ago
- Duplicate records in the same staging batch — needs upstream dedup
- Surrogate key generation at scale — here it's `MAX + row_number()`; in production use `GENERATED ALWAYS AS IDENTITY` (Unity Catalog) or a sequence
- Partitioning strategy for 100M+ customer tables
