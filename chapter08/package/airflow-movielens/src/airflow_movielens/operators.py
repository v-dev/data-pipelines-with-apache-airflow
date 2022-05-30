import json
import os

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

from .hooks import MovielensHook


class MovielensFetchRatingsOperator(BaseOperator):
    """
    Operator that fetches ratings from the Movielens API.

    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Movielens API. Connection
        is expected to include authentication details (login/password) and the
        host that is serving the API.
    output_path : str
        Path to write the fetched ratings to.
    start_date : str
        (Templated) start date to start fetching ratings from (inclusive).
        Expected format is YYYY-MM-DD (equal to Airflow's ds formats).
    end_date : str
        (Templated) end date to fetching ratings up to (exclusive).
        Expected format is YYYY-MM-DD (equal to Airflow's ds formats).
    batch_size : int
        Size of the batches (pages) to fetch from the API. Larger values
        mean less requests, but more data transferred per request.
    """

    template_fields = ("_start_date", "_end_date", "_output_path")

    @apply_defaults
    def __init__(
            self,
            conn_id,
            output_path,
            start_date="{{ds}}",
            end_date="{{next_ds}}",
            batch_size=1000,
            **kwargs,
    ):
        super(MovielensFetchRatingsOperator, self).__init__(**kwargs)

        self._conn_id = conn_id
        self._output_path = output_path
        self._start_date = start_date
        self._end_date = end_date
        self._batch_size = batch_size

    # pylint: disable=unused-argument,missing-docstring
    def execute(self, context):
        hook = MovielensHook(self._conn_id)

        try:
            self.log.info(
                f"Fetching ratings for {self._start_date} to {self._end_date}"
            )
            ratings = list(
                hook.get_ratings(
                    start_date=self._start_date,
                    end_date=self._end_date,
                    batch_size=self._batch_size,
                )
            )
            self.log.info(f"Fetched {len(ratings)} ratings")
        finally:
            # Make sure we always close our hook's session.
            hook.close()

        self.log.info(f"Writing ratings to {self._output_path}")

        # Make sure output directory exists.
        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)

        # Write output as JSON.
        with open(self._output_path, "w") as file_:
            json.dump(ratings, fp=file_)


class MovielensDownloadOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date", "_output_path")

    def __init__(
            self,
            conn_id,
            start_date,
            end_date,
            output_path,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._start_date = start_date
        self._end_date = end_date
        self._output_path = output_path

    def execute(self, context):
        with MovielensHook(self._conn_id) as hook:
            ratings = hook.get_ratings(
                start_date=self._start_date,
                end_date=self._end_date,
            )

        with open(self._output_path, "w") as f:
            f.write(json.dumps(ratings))


class MovielensToPostgresOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date", "_insert_query")

    def __init__(
            self,
            movielens_conn_id,
            start_date,
            end_date,
            postgres_conn_id,
            insert_query,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self._movielens_conn_id = movielens_conn_id
        self._start_date = start_date
        self._end_date = end_date
        self._postgres_conn_id = postgres_conn_id
        self._insert_query = insert_query

    def execute(self, context):
        with MovielensHook(self._movielens_conn_id) as movielens_hook:
            ratings = list(movielens_hook.get_ratings(
                start_date=self._start_date,
                end_date=self._end_date),
            )

        postgres_hook = PostgresHook(
            postgres_conn_id=self._postgres_conn_id
        )
        insert_queries = [
            self._insert_query.format(",".join([str(_[1]) for _ in sorted(rating.items())]))
            for rating in ratings
        ]
        postgres_hook.run(insert_queries)
