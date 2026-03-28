import pandas as pd
from airflow.sdk import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from pathlib import Path
import pendulum

# ── Constants ────────────────────────────────────────────────────────────────
RAW_FILE_PATH      = "/opt/airflow/data/raw/movie_ratings.csv"
EXPECTED_COLUMNS = ["id", "movie", "year", "imdb", "metascore", "votes"]
EXPECTED_ROW_COUNT = 1800

FAIL_PATH          = "/opt/airflow/data/fail/movie_ratings_errors.csv"
PROCESS_PATH       = "/opt/airflow/data/process/movie_ratings_clean.parquet"


# ── Task 1: check_input ───────────────────────────────────────────────────────
def check_input(ti):
    """
    Detection only — no cleaning performed here.
    Checking List:
      1. Column names remain unchanged (excluding the unnamed index column)
      2. No null values exist in any column
    Pushes:
      - raw_data        (JSON) ← full unmodified df for the No-issue path
      - has_issue       (bool)
      - bad_row_indices (list)
      - issue_summary   (list)
    """
    df = pd.read_csv(RAW_FILE_PATH)
    df.columns = [
        "id" if str(col).startswith("Unnamed:") else col
        for col in df.columns
    ]
    print(f"[Check_Input] Columns after rename: {df.columns.tolist()}")
    issues = []

    # ── Check 1: Column names unchanged ──────────────────────────────────────
    actual_columns = df.columns.tolist()
    if actual_columns != EXPECTED_COLUMNS:
        missing = set(EXPECTED_COLUMNS) - set(actual_columns)
        extra   = set(actual_columns)   - set(EXPECTED_COLUMNS)
        issues.append(
            f"Schema mismatch | missing={missing} | extra={extra}"
        )
    else:
        print(f"[Check_Input] ✓ Column names OK: {actual_columns}")

    # ── Check 2: Null values ──────────────────────────────────────────────────
    null_mask       = df.isnull().any(axis=1)
    bad_row_indices = df[null_mask].index.tolist()
    null_row_count  = len(bad_row_indices)

    if null_row_count > 0:
        issues.append(
            f"Null values found in {null_row_count} row(s)"
        )
    else:
        print(f"[Check_Input] ✓ No null values found")

    # ── XCom push ─────────────────────────────────────────────────────────────
    has_issue = len(issues) > 0
    ti.xcom_push(key="raw_data",        value=df.to_json(orient="split"))  # ← no cleaning yet
    ti.xcom_push(key="has_issue",       value=has_issue)
    ti.xcom_push(key="bad_row_indices", value=bad_row_indices)
    ti.xcom_push(key="issue_summary",   value=issues)

    print(f"[Check_Input] has_issue={has_issue} | issues={issues}")


# ── Task 2: issue_found (BranchPythonOperator) ────────────────────────────────
def route_by_issue(ti):
    """
    Decision diamond: Issue Found?
      Yes → split_record
      No  → convert_to_parquet
    """
    has_issue = ti.xcom_pull(task_ids="check_input", key="has_issue")
    return "split_record" if has_issue else "convert_to_parquet"


# ── Task 3: split_record ──────────────────────────────────────────────────────
def split_record(ti):
    """
    Issue path (Yes branch):
      1. Read raw file directly, rename first column to "id", save error rows as-is → fail/
      2. Clean the full dataframe from XCom (drop null rows)
      3. Push processed_data XCom → consumed by convert_to_parquet
    """
    bad_row_indices = ti.xcom_pull(task_ids="check_input", key="bad_row_indices")

    # ── Step 1: Backup — read raw file, rename only, no other changes ─────────
    df_raw = pd.read_csv(RAW_FILE_PATH)
    df_raw.columns = [
        "id" if str(col).startswith("Unnamed:") else col
        for col in df_raw.columns
    ]
    df_fail = df_raw.loc[bad_row_indices]
    Path(FAIL_PATH).parent.mkdir(parents=True, exist_ok=True)
    df_fail.to_csv(FAIL_PATH, index=False)
    print(f"[Split_Record] Backed up {len(df_fail)} raw error rows → {FAIL_PATH}")

    # ── Step 2: Clean — drop null rows from XCom data ────────────────────────
    raw_data = ti.xcom_pull(task_ids="check_input", key="raw_data")
    df       = pd.read_json(raw_data, orient="split")
    df_clean = df.drop(index=bad_row_indices)
    print(f"[Split_Record] Dropped {len(df_fail)} null rows | {len(df_clean)} rows remaining")

    # ── Step 3: Push cleaned data for convert_to_parquet ─────────────────────
    ti.xcom_push(key="processed_data", value=df_clean.to_json(orient="split"))
    print(f"[Split_Record] Pushed processed_data ({len(df_clean)} rows) to XCom")


# ── Task 4: convert_to_parquet ────────────────────────────────────────────────
def convert_to_parquet(ti):
    """
    Receives processed_data XCom from either:
      - split_record  (Yes branch: cleaned df, nulls removed)
      - check_input   (No branch:  raw_data used as-is since no issues found)
    Saves final output as parquet.
    """
    # Try pulling from split_record first (Yes branch), fall back to check_input (No branch)
    processed_data = (
        ti.xcom_pull(task_ids="split_record",  key="processed_data") or
        ti.xcom_pull(task_ids="check_input",   key="raw_data")
    )

    df = pd.read_json(processed_data, orient="split")

    Path(PROCESS_PATH).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PROCESS_PATH, index=False)

    print(f"[Convert_to_Parquet] Saved {len(df)} clean rows → {PROCESS_PATH}")


# ── DAG wiring ────────────────────────────────────────────────────────────────
with DAG(
    dag_id="movie_ratings_pipeline",
    start_date=pendulum.datetime(2026, 3, 28),
    schedule=None,
    catchup=False,
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    check_input_task = PythonOperator(
        task_id="check_input",
        python_callable=check_input,
    )

    branch_task = BranchPythonOperator(
        task_id="issue_found",
        python_callable=route_by_issue,
    )

    split_record_task = PythonOperator(
        task_id="split_record",
        python_callable=split_record,
    )

    convert_task = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
        trigger_rule="none_failed_min_one_success",  # ← runs after either branch
    )

    start >> check_input_task >> branch_task >> [split_record_task, convert_task]
    split_record_task >> convert_task >> end