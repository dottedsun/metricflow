import textwrap

from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile
from dbt_semantic_interfaces.objects.data_source import DataSourceOrigin, MutabilityType
from dbt_semantic_interfaces.objects.elements.entity import EntityType
from dbt_semantic_interfaces.objects.elements.dimension import DimensionType
from dbt_semantic_interfaces.parsing.dir_to_model import parse_yaml_files_to_model
from dbt_semantic_interfaces.objects.time_granularity import TimeGranularity


def test_base_data_source_attribute_parsing() -> None:
    """Test for parsing a data source specification without regard for measures, identifiers, or dimensions"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: base_property_test
          mutability:
            type: append_only
            type_params:
              min: minimum_value
              max: maximum_value
              update_cron: "* * 1 * *"
              along: dimension_column
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert data_source.name == "base_property_test"
    assert data_source.origin == DataSourceOrigin.SOURCE  # auto-filled from default, not user-configurable
    assert data_source.mutability.type == MutabilityType.APPEND_ONLY
    assert data_source.mutability.type_params is not None
    assert data_source.mutability.type_params.min == "minimum_value"
    assert data_source.mutability.type_params.max == "maximum_value"
    assert data_source.mutability.type_params.update_cron == "* * 1 * *"
    assert data_source.mutability.type_params.along == "dimension_column"


def test_data_source_metadata_parsing() -> None:
    """Test for asserting that internal metadata is parsed into the DataSource object"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: metadata_test
          mutability:
            type: immutable
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert data_source.metadata is not None
    assert data_source.metadata.repo_file_path == "test_dir/inline_for_test"
    assert data_source.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
        name: metadata_test
        mutability:
          type: immutable
        """
    )
    assert data_source.metadata.file_slice.content == expected_metadata_content


def test_data_source_sql_table_parsing() -> None:
    """Test for parsing a data source specification with a sql_table provided"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: sql_table_test
          mutability:
            type: immutable
          sql_table: "some_schema.source_table"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert data_source.sql_table == "some_schema.source_table"


def test_data_source_sql_query_parsing() -> None:
    """Test for parsing a data source specification with a sql_query provided"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: sql_query_test
          mutability:
            type: immutable
          sql_query: "SELECT * FROM some_schema.source_table"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert data_source.sql_query == "SELECT * FROM some_schema.source_table"


def test_data_source_dbt_model_parsing() -> None:
    """Test for parsing a data source specification with a dbt model provided"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: dbt_model_test
          mutability:
            type: immutable
          dbt_model: "dbt_source.some_model"
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert data_source.dbt_model == "dbt_source.some_model"


def test_data_source_identifier_parsing() -> None:
    """Test for parsing a basic identifier out of a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: identifier_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
              expr: example_id
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.identifiers) == 1
    identifier = data_source.identifiers[0]
    assert identifier.name == "example_identifier"
    assert identifier.type is EntityType.PRIMARY
    assert identifier.role == "test_role"
    assert identifier.expr == "example_id"


def test_data_source_identifier_metadata_parsing() -> None:
    """Test for parsing metadata for an identifier object defined in a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: identifier_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          identifiers:
            - name: example_identifier
              type: primary
              role: test_role
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.identifiers) == 1
    identifier = data_source.identifiers[0]
    assert identifier.metadata is not None
    assert identifier.metadata.repo_file_path == "test_dir/inline_for_test"
    assert identifier.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
      name: example_identifier
      type: primary
      role: test_role
      """
    )
    assert identifier.metadata.file_slice.content == expected_metadata_content


def test_data_source_measure_parsing() -> None:
    """Test for parsing a measure out of a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: measure_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          measures:
            - name: example_measure
              agg: count_distinct
              expr: example_input
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.measures) == 1
    measure = data_source.measures[0]
    assert measure.name == "example_measure"
    assert measure.agg is AggregationType.COUNT_DISTINCT
    assert measure.create_metric is not True
    assert measure.expr == "example_input"


def test_data_source_measure_metadata_parsing() -> None:
    """Test for parsing metadata for a measure object defined in a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: measure_metadata_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          measures:
            - name: example_measure_with_metadata
              agg: count_distinct
              expr: example_input
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.measures) == 1
    measure = data_source.measures[0]
    assert measure.metadata is not None
    assert measure.metadata.repo_file_path == "test_dir/inline_for_test"
    assert measure.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
      name: example_measure_with_metadata
      agg: count_distinct
      expr: example_input
      """
    )
    assert measure.metadata.file_slice.content == expected_metadata_content


def test_data_source_create_metric_measure_parsing() -> None:
    """Test for parsing a measure out of a data source specification when create metric is set"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: measure_parsing_create_metric_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          measures:
            - name: example_measure
              agg: count_distinct
              create_metric: true
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.measures) == 1
    measure = data_source.measures[0]
    assert measure.create_metric is True


def test_data_source_categorical_dimension_parsing() -> None:
    """Test for parsing a categorical dimension out of a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              expr: dimension_input
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.dimensions) == 1
    dimension = data_source.dimensions[0]
    assert dimension.name == "example_categorical_dimension"
    assert dimension.type is DimensionType.CATEGORICAL
    assert dimension.is_partition is not True


def test_data_source_partition_dimension_parsing() -> None:
    """Test for parsing a partition dimension out of a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              is_partition: true
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.dimensions) == 1
    dimension = data_source.dimensions[0]
    assert dimension.is_partition is True


def test_data_source_time_dimension_parsing() -> None:
    """Test for parsing a time dimension out of a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_time_dimension
              type: time
              type_params:
                time_granularity: month
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.dimensions) == 1
    dimension = data_source.dimensions[0]
    assert dimension.type is DimensionType.TIME
    assert dimension.type_params is not None
    assert dimension.type_params.is_primary is not True
    assert dimension.type_params.time_granularity is TimeGranularity.MONTH


def test_data_source_primary_time_dimension_parsing() -> None:
    """Test for parsing a primary time dimension out of a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_time_dimension
              type: time
              type_params:
                time_granularity: month
                is_primary: true
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.dimensions) == 1
    dimension = data_source.dimensions[0]
    assert dimension.type is DimensionType.TIME
    assert dimension.type_params is not None
    assert dimension.type_params.is_primary is True


def test_data_source_dimension_metadata_parsing() -> None:
    """Test for parsing metadata for an dimension object defined in a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: dimension_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: example_categorical_dimension
              type: categorical
              expr: dimension_input
        """
    )
    file = YamlConfigFile(filepath="test_dir/inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.dimensions) == 1
    dimension = data_source.dimensions[0]
    assert dimension.metadata is not None
    assert dimension.metadata.repo_file_path == "test_dir/inline_for_test"
    assert dimension.metadata.file_slice.filename == "inline_for_test"
    expected_metadata_content = textwrap.dedent(
        """\
      name: example_categorical_dimension
      type: categorical
      expr: dimension_input
      """
    )
    assert dimension.metadata.file_slice.content == expected_metadata_content


def test_data_source_dimension_validity_params_parsing() -> None:
    """Test for parsing dimension validity info out of a data source specification"""
    yaml_contents = textwrap.dedent(
        """\
        data_source:
          name: scd_parsing_test
          mutability:
            type: immutable
          sql_table: some_schema.source_table
          dimensions:
            - name: start_time_dimension
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_start: True
            - name: end_time_dimension
              type: time
              type_params:
                time_granularity: day
                validity_params:
                  is_end: True
        """
    )
    file = YamlConfigFile(filepath="inline_for_test", contents=yaml_contents)

    build_result = parse_yaml_files_to_model(files=[file])

    assert len(build_result.model.data_sources) == 1
    data_source = build_result.model.data_sources[0]
    assert len(data_source.dimensions) == 2
    start_dimension = data_source.dimensions[0]
    assert start_dimension.type_params is not None
    assert start_dimension.type_params.validity_params is not None
    assert start_dimension.type_params.validity_params.is_start is True
    assert start_dimension.type_params.validity_params.is_end is False
    end_dimension = data_source.dimensions[1]
    assert end_dimension.type_params is not None
    assert end_dimension.type_params.validity_params is not None
    assert end_dimension.type_params.validity_params.is_start is False
    assert end_dimension.type_params.validity_params.is_end is True
