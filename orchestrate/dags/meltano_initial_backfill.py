# This file is managed by the 'airflow' file bundle and updated automatically when `meltano upgrade` is run.
# To prevent any manual changes from being overwritten, remove the file bundle from `meltano.yml` or disable automatic updates:
#     meltano config --plugin-type=files airflow set _update orchestrate/dags/meltano.py false

# If you want to define a custom DAG, create
# a new file under orchestrate/dags/ and Airflow
# will pick it up automatically.

import os
import logging
import subprocess
import json
import pendulum

from airflow import DAG
try:
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.dummy_operator import DummyOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from datetime import timedelta
from pathlib import Path


logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "concurrency": 1,
}

DEFAULT_TAGS = ["meltano"]

project_root = os.getenv("MELTANO_PROJECT_ROOT", os.getcwd())

meltano_bin = ".meltano/run/bin"

if not Path(project_root).joinpath(meltano_bin).exists():
    logger.warning(
        f"A symlink to the 'meltano' executable could not be found at '{meltano_bin}'. Falling back on expecting it to be in the PATH instead."
    )
    meltano_bin = "meltano"

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "owner": "airflow",
    "start_date": pendulum.parse("2020-01-01"),
}

# Create the DAG
dag = DAG("degreed_backfill", default_args=default_args, schedule_interval=None)



def generate_set_config_command(start_date):


    return BashOperator(
        task_id=f"set_config_{pendulum.parse(start_date).format('YYYY-MM-DD')}",
        bash_command=f"""
        cd {project_root}; 
        {meltano_bin} config --plugin-type=extractor tap-degreed set --store=meltano_yml start_date {start_date};
        """,
        dag=dag
    )


dummy_operator = DummyOperator(task_id="start", dag=dag)


elt = BashOperator(
        task_id="extract_load",
        bash_command=f"cd {project_root}; {meltano_bin} elt tap-degreed loader-bigquery --job_id=degreed-to-bigquery",
        dag=dag,
    )

for day in (pendulum.period(pendulum.parse("2021-11-01"), pendulum.yesterday('utc').replace(microsecond=0)).range('days')):
    dummy_operator >> generate_set_config_command(day.isoformat()) >> elt


