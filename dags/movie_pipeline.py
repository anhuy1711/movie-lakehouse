import glob
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SUBMIT = ("docker exec spark-master /opt/spark/bin/spark-submit "
          "--master spark://spark-master:7077 --conf spark.driver.host=spark-master")
JOBS = "/opt/project/src/jobs"
GOLD = "/opt/project/src/gold"
CONFIG_ROOT = "/opt/airflow/config"      # thư mục config đã được mount vào Airflow
P_DATE = " --p-date {{ ds }}"            # Airflow tự thay {{ ds }} bằng ngày chạy


def config_names(layer):
    files = glob.glob(os.path.join(CONFIG_ROOT, layer, "*.yaml"))
    return sorted(os.path.basename(f)[:-5] for f in files)


default_args = {"retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="movie_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["movie-lakehouse"],
) as dag:

    fetch_tmdb = BashOperator(
        task_id="fetch_tmdb",
        bash_command="docker exec -u root spark-master python3 /opt/project/scripts/fetch_tmdb.py"
                     + P_DATE + " --limit 300",
    )

    bronze_tasks = [
        BashOperator(task_id=f"bronze_{n}",
                     bash_command=f"{SUBMIT} {JOBS}/bronze_job.py --table {n}" + P_DATE)
        for n in config_names("bronze")
    ]

    silver_tasks = [
        BashOperator(task_id=f"silver_{n}",
                     bash_command=f"{SUBMIT} {JOBS}/silver_job.py --table {n}" + P_DATE)
        for n in config_names("silver")
    ]

    gold_dim = BashOperator(task_id="gold_dim_movie",
                            bash_command=f"{SUBMIT} {GOLD}/dim_movie.py")
    gold_fact_rating = BashOperator(task_id="gold_fact_rating",
                                    bash_command=f"{SUBMIT} {GOLD}/fact_rating.py")
    gold_snapshot = BashOperator(task_id="gold_fact_movie_daily_snapshot",
                                 bash_command=f"{SUBMIT} {GOLD}/fact_movie_daily_snapshot.py" + P_DATE)
    gold_agg = BashOperator(task_id="gold_agg_genre_stats",
                            bash_command=f"{SUBMIT} {GOLD}/agg_genre_stats.py")

    fetch_tmdb >> bronze_tasks
    for b in bronze_tasks:
        for s in silver_tasks:
            b >> s
    for s in silver_tasks:
        s >> gold_dim
    gold_dim >> [gold_fact_rating, gold_snapshot] >> gold_agg