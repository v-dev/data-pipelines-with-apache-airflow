"""
https://www.astronomer.io/guides/testing-airflow/
"""
from airflow.models import DagBag
from assertpy import assert_that


def test_no_import_errors():
    dag_bag = DagBag()
    print(f"dags: {dag_bag.dags}")
    assert_that(dag_bag.import_errors).is_length(0)
