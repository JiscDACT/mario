"""
Common output options for different data formats along with their default values
"""


class OutputOptions:
    def __init__(self, **kwargs):
        self.minimise = kwargs.get('minimise', False)
        self.validate = kwargs.get('validate', False)
        self.allow_nulls = kwargs.get('allow_nulls', False)
        self.chunk_size = kwargs.get('chunk_size', 100000)
        self.include_row_numbers = kwargs.get('include_row_numbers', False)


class CsvOptions(OutputOptions):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.compress_using_gzip = kwargs.get('compress_using_gzip', False)


class HyperOptions(OutputOptions):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.table = kwargs.get('table', 'Extract')
        self.schema = kwargs.get('schema', 'Extract')
