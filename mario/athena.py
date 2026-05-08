from pyathena import connect
from mario.data_extractor import DataExtractor, Configuration
import logging
import datetime
import numbers
import re

from mario.mapping import rewrite_csv_header_with_fieldmapping
from mario.options import CsvOptions
from mario.utils import gzip_file

logger = logging.getLogger(__name__)


class AthenaConfiguration(Configuration):
    """
    Extended configuration
    """

    def __init__(self):
        super().__init__()
        self.aws_s3_staging_dir = None
        self.aws_region_name = None
        self.aws_athena_workgroup = 'primary'
        self.catalog = 'awsdatacatalog'
        self.query_format = 'snake_case'


class AthenaDataExtractor(DataExtractor):
    """
    Streaming extractor using Athena + PyAthena.
    Extends StreamingDataExtractor; only get_connection() is Athena-specific.
    """

    def __init__(self, configuration: AthenaConfiguration, dataset_specification, metadata):
        super().__init__(configuration, dataset_specification, metadata)
        self.configuration = configuration

    def get_connection(self):
        """
        PyAthena provides a DBAPI connection compatible with pandas.read_sql().
        """
        cfg = self.configuration

        if cfg.hook:
            # If the user provided a hook, delegate to it
            # See: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/athena_sql/index.html
            return cfg.hook.get_conn()

        # Expect these in configuration:
        return connect(
            s3_staging_dir=cfg.aws_s3_staging_dir,
            region_name=cfg.aws_region_name,
            work_group=cfg.aws_athena_workgroup,
            schema_name=cfg.schema,
            catalog_name=cfg.catalog
        )

    def get_total(self, measure=None):
        """
        For totals when streaming data we need to run a totals SQL query separate
        from the main query and use the results of this
        :return: the total value of the query
        NOTE this is a direct copy from StreamingDataExtractor
        """
        from pandas import read_sql
        logger.info("Building totals query")
        measure = self.__get_measure__(measure)
        if self.configuration.query_builder is not None:
            from mario.query_builder import QueryBuilder
            query_builder: QueryBuilder = self.configuration.query_builder(
                configuration=self.configuration,
                metadata=self.metadata,
                dataset_specification=self.dataset_specification)
            totals_query = query_builder.create_totals_query(measure=measure)
        else:
            raise NotImplementedError

        totals_df = read_sql(totals_query[0], self.get_connection(), params=totals_query[1])
        return totals_df.iat[0, 0]

    def run_query(self) -> str:
        """
        Runs the SQL query in Athena and returns the
        result path in S3
        :return: the S3 path where the result is stored
        """
        import awswrangler as wr
        import boto3

        # Build SQL
        self.__build_query__()
        raw_sql, parameters = self._query
        sql = interpolate_athena_sql(raw_sql, parameters)
        cfg = self.configuration

        # 1. Run SQL via Wrangler
        qid = wr.athena.start_query_execution(
            sql=sql,
            database=cfg.schema,
            workgroup=cfg.aws_athena_workgroup,
            s3_output=cfg.aws_s3_staging_dir,
        )
        wr.athena.wait_query(qid)

        # 2. Get S3 CSV result path
        client = boto3.client("athena")
        meta = client.get_query_execution(QueryExecutionId=qid)
        s3_uri = meta["QueryExecution"]["ResultConfiguration"]["OutputLocation"]

        return s3_uri

    def save_data_as_csv(self, file_path: str, **kwargs):
        """
        Athena doesn't support streaming, but natively saves CSV files
        in S3 as output, so we really don't need to do anything else
        other than run the query and download the results from S3
        :param file_path:
        :param kwargs:
        :return:
        """
        import awswrangler as wr

        # Parse options
        options = CsvOptions(**kwargs)

        # Run query and get output location
        s3_uri = self.run_query()

        # Download raw Athena CSV
        wr.s3.download(path=s3_uri, local_file=file_path)

        # Rewrite header with FieldMapping
        rewrite_csv_header_with_fieldmapping(file_path, self.mapping)

        if options.compress_using_gzip:
            gz_path = gzip_file(file_path)
            return gz_path

        return file_path


ATHENA_PARAM_PATTERN = re.compile(r'%\((?P<name>[a-zA-Z0-9_]+)\)s')


def athena_escape_string(value: str) -> str:
    """
    Escape a Python string for safe Athena SQL literal usage.
    Athena uses standard SQL single-quoted strings; internal quotes doubled.
    """
    return value.replace("'", "''")


def athena_literal(value):
    """
    Convert a Python value into an Athena-safe SQL literal.
    """
    # None -> NULL
    if value is None:
        return "NULL"

    # Boolean -> true/false
    if isinstance(value, bool):
        return "true" if value else "false"

    # Number -> raw
    if isinstance(value, numbers.Number):
        return str(value)

    # Date / datetime
    if isinstance(value, (datetime.date, datetime.datetime)):
        return f"'{value.isoformat()}'"

    # List/tuple -> comma-separated list of literals
    if isinstance(value, (list, tuple)):
        return ", ".join(athena_literal(v) for v in value)

    # Everything else -> string literal
    return f"'{athena_escape_string(str(value))}'"


def interpolate_athena_sql(sql: str, params: dict):
    """
    Replace all %(name)s placeholders using Athena-safe literals.
    """

    def repl(match):
        name = match.group("name")
        if name not in params:
            raise KeyError(f"Missing SQL parameter: {name}")
        return athena_literal(params[name])

    return ATHENA_PARAM_PATTERN.sub(repl, sql)