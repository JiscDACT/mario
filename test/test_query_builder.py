from mario.query_builder import ViewBasedQueryBuilder, SubsetQueryBuilder
from mario.data_extractor import Configuration
from mario.metadata import Metadata, Item
from mario.dataset_specification import DatasetSpecification, Constraint


def test_query_builder():
    dataset_specification = DatasetSpecification()

    metadata = Metadata()

    configuration = Configuration()
    configuration.schema = 'dbo'
    configuration.view = 'v_extract_test'

    query_builder = ViewBasedQueryBuilder(
        dataset_specification=dataset_specification,
        metadata=metadata,
        configuration=configuration
    )

    query = query_builder.create_query()
    assert query[0] == 'SELECT * FROM [dbo].[v_extract_test] WITH(NOLOCK)'


def test_subset_query_builder():
    dataset_specification = DatasetSpecification()
    dataset_specification.dimensions.append('fruit')
    dataset_specification.measures.append('volume')

    metadata = Metadata()
    item = Item()
    item.name = 'fruit'
    metadata.add_item(item)

    configuration = Configuration()
    configuration.schema = 'dbo'
    configuration.view = 'v_extract_test'

    query_builder = SubsetQueryBuilder(
        dataset_specification=dataset_specification,
        metadata=metadata,
        configuration=configuration
    )

    query = query_builder.create_query()
    assert query[0] == 'SELECT "fruit",SUM("volume") "volume" FROM "dbo"."v_extract_test" ' \
                       'GROUP BY "fruit"'


def test_subset_query_builder_multiple_measures():
    dataset_specification = DatasetSpecification()
    dataset_specification.dimensions.append('fruit')
    dataset_specification.measures.append('volume')
    dataset_specification.measures.append('weight')

    metadata = Metadata()
    item = Item()
    item.name = 'fruit'
    metadata.add_item(item)

    configuration = Configuration()
    configuration.schema = 'dbo'
    configuration.view = 'v_extract_test'

    query_builder = SubsetQueryBuilder(
        dataset_specification=dataset_specification,
        metadata=metadata,
        configuration=configuration
    )

    query = query_builder.create_query()
    assert query[0] == 'SELECT "fruit",SUM("volume") "volume",SUM("weight") "weight" ' \
                       'FROM "dbo"."v_extract_test" GROUP BY "fruit"'


def test_subset_query_builder_with_constraint():
    dataset_specification = DatasetSpecification()
    dataset_specification.dimensions.append('fruit')
    dataset_specification.dimensions.append('colour')
    dataset_specification.measures.append('volume')
    constraint = Constraint()
    constraint.item = 'colour'
    constraint.allowed_values=['yellow', 'orange']
    dataset_specification.constraints.append(constraint)

    metadata = Metadata()
    item = Item()
    item.name = 'fruit'
    metadata.add_item(item)

    configuration = Configuration()
    configuration.schema = 'dbo'
    configuration.view = 'v_extract_test'

    query_builder = SubsetQueryBuilder(
        dataset_specification=dataset_specification,
        metadata=metadata,
        configuration=configuration
    )

    query = query_builder.create_query()
    assert query[0] == 'SELECT "fruit","colour",SUM("volume") "volume" FROM "dbo"."v_extract_test" ' \
                       'WHERE "colour" IN (?,?) GROUP BY "fruit","colour"'
    assert query[1] == ['yellow', 'orange']