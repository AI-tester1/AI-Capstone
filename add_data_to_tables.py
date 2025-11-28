
#!/usr/bin/env python3
"""
Load three CSVs into Postgres:
  - gdpr_text.csv           -> table gdpr_text
  - gdpr_violations_with_derivatives_clean.csv -> table gdpr_violations_derived
  - GDPRdataset.csv         -> table GDPR_dataset (CSV headers have spaces)

Run: python load_gdpr_csvs.py
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import date
from decimal import Decimal

# -----------------------------------------------------------------------------
# 1) Configuration
# -----------------------------------------------------------------------------
DB_PARAMS = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "mypwd1"),
    "dbname": os.getenv("PGDATABASE", "capstone"),
}

# Adjust paths to where your CSVs live (Codespaces example paths)
CSV_GDPR_TEXT        = os.getenv("CSV_GDPR_TEXT",        "/workspaces/AI-Capstone/gdpr_text.csv")
CSV_GDPR_VIOLATIONS  = os.getenv("CSV_GDPR_VIOLATIONS",  "/workspaces/AI-Capstone/gdpr_violations_with_derivatives_clean.csv")
CSV_GDPR_DATASET     = os.getenv("CSV_GDPR_DATASET",     "/workspaces/AI-Capstone/GDPRdataset.csv")

# Batch size for inserts
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))

# -----------------------------------------------------------------------------
# 2) Helpers for parsing
# -----------------------------------------------------------------------------
def to_int(val):
    """Convert to int or return None for NaN/blank."""
    try:
        if pd.isna(val) or str(val).strip() == "":
            return None
        return int(str(val).strip())
    except Exception:
        return None

def to_text(val):
    """Normalize text; keep empty string if present."""
    if pd.isna(val):
        return ""
    return str(val).strip()

def to_numeric(val):
    """Convert to Decimal for NUMERIC columns; return None for NaN/blank."""
    try:
        if pd.isna(val) or str(val).strip() == "":
            return None
        return Decimal(str(val).replace(",", "").strip())
    except Exception:
        return None

def to_date(val):
    """Parse CSV date into Python date; return None if parsing fails."""
    try:
        if pd.isna(val) or str(val).strip() == "":
            return None
        dt = pd.to_datetime(val, infer_datetime_format=True, errors="coerce")
        return None if pd.isna(dt) else dt.date()
    except Exception:
        return None

def chunk_iter(seq, size):
    """Yield chunks of a sequence."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]

# -----------------------------------------------------------------------------
# 3) Loaders & Inserters for each table
# -----------------------------------------------------------------------------
def load_gdpr_text(conn, csv_path):
    """
    CSV columns expected:
      chapter, chapter_title, article, article_title, sub_article, gdpr_text, href
    Target table: gdpr_text (same column names)
    """
    print(f"\n[gdpr_text] Reading: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)

    required = ["chapter", "chapter_title", "article", "article_title", "sub_article", "gdpr_text", "href"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"gdpr_text: missing columns in CSV: {missing}")

    rows = []
    for _, r in df.iterrows():
        rows.append((
            to_int(r["chapter"]),
            to_text(r["chapter_title"]),
            to_int(r["article"]),
            to_text(r["article_title"]),
            to_text(r["sub_article"]),     # TEXT; blank allowed
            to_text(r["gdpr_text"]),
            to_text(r["href"])
        ))

    print(f"[gdpr_text] Rows prepared: {len(rows)}")

    sql = """
        INSERT INTO gdpr_text (
            chapter, chapter_title, article, article_title, sub_article, gdpr_text, href
        )
        VALUES %s
    """
    with conn.cursor() as cur:
        for chunk in chunk_iter(rows, BATCH_SIZE):
            execute_values(cur, sql, chunk)
        conn.commit()
    print("[gdpr_text] Insert complete.")


def load_gdpr_violations_derived(conn, csv_path):
    """
    CSV headers (as generated earlier):
      id, picture, name, price, authority, date, controller,
      article_violated, type, source, summary, Article_no_der, Sub_article_no_der

    Target table: gdpr_violations_derived with columns:
      id, picture_path, country_name, fine_price, authority, date, controller,
      article_violated, type, source, summary, article_no_der, sub_article_no_der
    """
    print(f"\n[gdpr_violations_derived] Reading: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)

    required = [
        "id","picture","name","price","authority","date","controller",
        "article_violated","type","source","summary","Article_no_der","Sub_article_no_der"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"gdpr_violations_derived: missing columns in CSV: {missing}")

    rows = []
    for _, r in df.iterrows():
        rows.append((
            to_int(r["id"]),
            to_text(r["picture"]),           # -> picture_path
            to_text(r["name"]),              # -> country_name
            to_numeric(r["price"]),          # -> fine_price (NUMERIC)
            to_text(r["authority"]),
            to_date(r["date"]),              # -> DATE
            to_text(r["controller"]),
            to_text(r["article_violated"]),
            to_text(r["type"]),
            to_text(r["source"]),
            to_text(r["summary"]),
            to_int(r["Article_no_der"]),     # -> INTEGER
            to_text(r["Sub_article_no_der"]) # -> TEXT; keep blank "" when absent
        ))

    print(f"[gdpr_violations_derived] Rows prepared: {len(rows)}")

    sql = """
        INSERT INTO gdpr_violations_derived (
            id, picture_path, country_name, fine_price, authority, date, controller,
            article_violated, type, source, summary, article_no_der, sub_article_no_der
        )
        VALUES %s
    """
    with conn.cursor() as cur:
        for chunk in chunk_iter(rows, BATCH_SIZE):
            execute_values(cur, sql, chunk)
        conn.commit()
    print("[gdpr_violations_derived] Insert complete.")


def load_gdpr_dataset(conn, csv_path):
    """
    CSV headers (with spaces):
      Content,
      Article Number,
      Article Name,
      Chapter Number,
      Chapter Name,
      Article Word Count,
      Question,
      Answer,
      Question Word Count,
      Answer Word Count

    Target table: GDPR_dataset (underscored column names):
      Content, Article_Number, Article_Name, Chapter_Number, Chapter_Name,
      Article_Word_Count, Question, Answer, Question_Word_Count, Answer_Word_Count
    """
    print(f"\n[GDPR_dataset] Reading: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)

    csv_headers = [
        "Content",
        "Article Number",
        "Article Name",
        "Chapter Number",
        "Chapter Name",
        "Article Word Count",
        "Question",
        "Answer",
        "Question Word Count",
        "Answer Word Count",
    ]
    missing = [c for c in csv_headers if c not in df.columns]
    if missing:
        raise ValueError(f"GDPR_dataset: missing columns in CSV: {missing}")

    # Map CSV headers -> table columns
    rows = []
    for _, r in df.iterrows():
        rows.append((
            to_text(r["Content"]),
            to_int(r["Article Number"]),
            to_text(r["Article Name"]),
            to_int(r["Chapter Number"]),
            to_text(r["Chapter Name"]),
            to_int(r["Article Word Count"]),
            to_text(r["Question"]),
            to_text(r["Answer"]),
            to_int(r["Question Word Count"]),
            to_int(r["Answer Word Count"]),
        ))

    print(f"[GDPR_dataset] Rows prepared: {len(rows)}")

    sql = """
        INSERT INTO GDPR_dataset (
            Content, Article_Number, Article_Name, Chapter_Number, Chapter_Name,
            Article_Word_Count, Question, Answer, Question_Word_Count, Answer_Word_Count
        )
        VALUES %s
    """
    with conn.cursor() as cur:
        for chunk in chunk_iter(rows, BATCH_SIZE):
            execute_values(cur, sql, chunk)
        conn.commit()
    print("[GDPR_dataset] Insert complete.")


# -----------------------------------------------------------------------------
# 4) Main
# -----------------------------------------------------------------------------
def main():
    # Basic file existence check
    for path in [CSV_GDPR_TEXT, CSV_GDPR_VIOLATIONS, CSV_GDPR_DATASET]:
        if not os.path.exists(path):
            print(f"ERROR: CSV not found -> {path}")
            sys.exit(1)

    conn = None
    try:
        print(f"Connecting to Postgres {DB_PARAMS['host']}:{DB_PARAMS['port']} db={DB_PARAMS['dbname']} ...")
        conn = psycopg2.connect(**DB_PARAMS)
        print("Connected.")

        # Load each dataset
        load_gdpr_text(conn, CSV_GDPR_TEXT)
        load_gdpr_violations_derived(conn, CSV_GDPR_VIOLATIONS)
        load_gdpr_dataset(conn, CSV_GDPR_DATASET)

        print("\nAll inserts completed successfully.\n")

    except Exception as e:
        print(f"\nAn error occurred: {e}\n")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()
