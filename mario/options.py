"""
Common output options for different data formats along with their default values
"""
import logging
logger = logging.getLogger(__name__)


class OutputOptions:
    def __init__(self, **kwargs):
        self.minimise = kwargs.get('minimise', False)
        self.validate = kwargs.get('validate', False)
        self.allow_nulls = kwargs.get('allow_nulls', True)
        self.chunk_size = kwargs.get('chunk_size', 100000)
        self.include_row_numbers = kwargs.get('include_row_numbers', False)

        if self.chunk_size <= 0:
            raise ValueError("Chunk size must be a positive integer")

        if not self.allow_nulls and not self.validate:
            logger.error("Inconsistent options chosen: "
                         "if you choose not to allow nulls, but don't enable validation, "
                         "the output may still have nulls")
            raise ValueError("Inconsistent output configuration")


class CsvOptions(OutputOptions):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.compress_using_gzip = kwargs.get('compress_using_gzip', False)


class HyperOptions(OutputOptions):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = kwargs.get('table', 'Extract')
        self.schema = kwargs.get('schema', 'Extract')


class ExcelOptions(OutputOptions):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.template_path = kwargs.get('template_path', 'excel_template.xlsx')
