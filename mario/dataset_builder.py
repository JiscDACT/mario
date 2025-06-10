import logging
import os.path
import shutil
import tempfile
from enum import Enum


from mario.data_extractor import DataExtractor
from mario.dataset_specification import DatasetSpecification
from mario.metadata import Metadata

logger = logging.getLogger(__name__)


class Format(Enum):
    TABLEAU_PACKAGED_DATASOURCE = 'tdsx'
    POWER_BI_PACKAGE = 'pbix'
    EXCEL_PIVOT = 'xlsx'
    CSV = 'csv'
    EXCEL_INFO_SHEET = 'info'
    HYPER = 'hyper'


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

    def remove_redundant_hierarchies(self):
        """
        Modifies the metadata to remove any hierarchies that contain
        only one item.
        :return: None
        """
        for hierarchy in self.metadata.get_hierarchies():
            items = self.metadata.get_hierarchy(hierarchy)
            items = [item for item in items if item in self.dataset_specification.items]
            if len(items) == 1:
                for item in self.metadata.items:
                    if 'hierarchies' in item.properties:
                        item.set_property('hierarchies',  [h for h in item.get_property('hierarchies') if h['hierarchy'] != hierarchy])

    def build(self, output_format: Format, file_path: str, **kwargs):
        if output_format == Format.TABLEAU_PACKAGED_DATASOURCE:
            self.__build_tdsx__(file_path, **kwargs)
        elif output_format == Format.CSV:
            self.__build_csv__(file_path, **kwargs)
        elif output_format == Format.EXCEL_PIVOT:
            self.__build_excel_pivot__(file_path, **kwargs)
        elif output_format == Format.EXCEL_INFO_SHEET:
            self.__build_excel_info_sheet(file_path, **kwargs)
        elif output_format == Format.HYPER:
            self.__build_hyper__(file_path, **kwargs)
        else:
            raise NotImplementedError

    def __build_hyper__(self, file_path: str, **kwargs):
        self.data.save_data_as_hyper(file_path=file_path, **kwargs)

    def __build_excel_info_sheet(self, file_path: str, **kwargs):
        from mario.excel_builder import ExcelBuilder
        excel_builder = ExcelBuilder(
            output_file_path=file_path,
            dataset_specification=self.dataset_specification,
            metadata=self.metadata,
            data_extractor=self.data,
            **kwargs
        )
        excel_builder.create_notes_only()

    def __build_excel_pivot__(self, file_path: str, **kwargs):
        from mario.excel_builder import ExcelBuilder
        if self.data.get_total() > 1000000:
            logger.warning("The dataset is larger than 1m rows; this isn't supported in Excel format")
        excel_builder = ExcelBuilder(
            output_file_path=file_path,
            dataset_specification=self.dataset_specification,
            metadata=self.metadata,
            data_extractor=self.data,
            **kwargs
        )
        excel_builder.create_workbook()

    def __build_csv__(self, file_path: str, **kwargs):
        # TODO export Info sheet as well - see code in Automation2.0 and TDSA.
        self.data.save_data_as_csv(file_path=file_path, **kwargs)

    def __build_tdsx__(self, file_path: str, **kwargs):
        from mario.hyper_utils import get_default_table_and_schema
        from tableau_builder.json_metadata import JsonRepository
        with tempfile.TemporaryDirectory() as temp_folder:

            # Metadata - convert to internal format used for Tableau
            metadata_file_path = os.path.join(temp_folder, 'metadata.json')
            self.metadata.save(metadata_file_path)
            repository = JsonRepository(metadata_file_path)

            # Move the hyper extract
            data_file_path = os.path.join(temp_folder, 'data.hyper')
            self.data.save_data_as_hyper(file_path=data_file_path, **kwargs)

            # Get the table and schema name from the extract
            schema, table = get_default_table_and_schema(data_file_path)

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
                table_name=table,
                schema_name=schema,
                use_metadata_groups=True
            )
            shutil.copyfile(src=output_path + '.tdsx', dst=file_path)
