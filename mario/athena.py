from pyathena import connect
from mario.data_extractor import StreamingDataExtractor, Configuration
import logging

logger = logging.getLogger(__name__)


class AthenaConfiguration(Configuration):
    """
    Extended configuration
    """

    def __init__(self):
        super().__init__()
        self.aws_s3_staging_dir = 's3://celeste-iceberg/athena-results/'
        self.aws_region_name = 'eu-west-2'
        self.aws_athena_workgroup = 'primary'
        self.catalog = 'awsdatacatalog'
        self.query_format = 'snake_case'


class AthenaStreamingDataExtractor(StreamingDataExtractor):
    """
    Streaming extractor using Athena + PyAthena.
    Extends StreamingDataExtractor; only get_connection() is Athena-specific.
    """

    def __init__(self, configuration: AthenaConfiguration, dataset_specification, metadata):
        super().__init__(configuration, dataset_specification, metadata)
        self.configuration = configuration

    def get_connection(self):
        """
        PyAthena provides a DBAPI connection compatible with pandas.read_sql() including chunksize.
        """
        cfg = self.configuration

        if cfg.hook:
            # If the user provided a hook, delegate to it
            return cfg.hook.get_conn()

        # Expect these in configuration:
        return connect(
            s3_staging_dir=cfg.aws_s3_staging_dir,
            region_name=cfg.aws_region_name,
            work_group=cfg.aws_athena_workgroup,
            schema_name=cfg.schema,
            catalog_name=cfg.catalog
    )
