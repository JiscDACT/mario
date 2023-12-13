import logging
import os.path
import shutil
import tempfile
from enum import Enum

from tableau_builder.hyper_utils import get_default_table_and_schema
from tableau_builder.json_metadata import JsonRepository

from mario.data_extractor import DataExtractor
from mario.dataset_specification import DatasetSpecification
from mario.metadata import Metadata

logger = logging.getLogger(__name__)


class Format(Enum):
    TABLEAU_PACKAGED_DATASOURCE = 'tdsx'
    POWER_BI_PACKAGE = 'pbix'
    EXCEL_PIVOT = 'xlsx'
    CSV = 'csv'


class DatasetBuilder:

    def __init__(self,
                 dataset_specification: DatasetSpecification = None,
                 metadata: Metadata = None,
                 data: DataExtractor = None
                 ):
        self.dataset_specification = dataset_specification
        self.metadata = metadata
        self.data = data

    def validate_metadata(self):
        """ Checks whether the metadata matches the specification"""
        not_included = []
        fields = self.dataset_specification.items

        for field in fields:
            try:
                self.metadata.get_metadata(field)
            except ValueError:
                not_included.append(field)

        if len(not_included) > 0:
            logger.error(self.dataset_specification.name + ": Some metadata is missing")
            logger.error(not_included)
            return False
        else:
            logger.info(self.dataset_specification.name + ": All metadata present")
            return True

    def build(self, output_format: Format, file_path: str, template_path: str = None):
        if output_format == Format.TABLEAU_PACKAGED_DATASOURCE:
            self.__build_tdsx__(file_path)
        elif output_format == Format.CSV:
            self.__build_csv__(file_path)
        elif output_format == Format.EXCEL_PIVOT:
            self.__build_excel_pivot__(file_path, template_path)
        else:
            raise NotImplementedError

    def __build_excel_pivot__(self, file_path: str, template_path: str):
        from mario.excel_builder import ExcelBuilder
        excel_builder = ExcelBuilder(
            output_file_path=file_path,
            dataset_specification=self.dataset_specification,
            metadata=self.metadata,
            data_extractor=self.data,
            template_path=template_path
        )
        excel_builder.create_workbook()

    def __build_csv__(self, file_path: str):
        # TODO export Info sheet as well - see code in Automation2.0 and TDSA.
        self.data.save_data_as_csv(file_path=file_path)

    def __build_tdsx__(self, file_path: str):
        with tempfile.TemporaryDirectory() as temp_folder:

            # Metadata - convert to internal format used for Tableau
            metadata_file_path = os.path.join(temp_folder, 'metadata.json')
            self.metadata.save(metadata_file_path)
            repository = JsonRepository(metadata_file_path)

            # Move the hyper extract
            data_file_path = os.path.join(temp_folder, 'data.hyper')
            self.data.save_data_as_hyper(file_path=data_file_path)

            # Get the table and schema name from the extract
            table_schema = get_default_table_and_schema(data_file_path)

            # Save the spec
            dataset_file_path = os.path.join(temp_folder, 'dataset.json')
            self.dataset_specification.save(dataset_file_path)

            # Build a tdsx in a temp file
            output_path = os.path.join(temp_folder, 'build')
            from tableau_builder.dataset import create_tdsx, HYPER
            create_tdsx(
                dataset_file=dataset_file_path,
                metadata_repository=repository,
                data_file=data_file_path,
                output_file=output_path,
                data_source_type=HYPER,
                table_name=table_schema['table'],
                schema_name=table_schema['schema'],
                use_metadata_groups=True
            )
            shutil.copyfile(src=output_path + '.tdsx', dst=file_path)
