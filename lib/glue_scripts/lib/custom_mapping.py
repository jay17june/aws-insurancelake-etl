# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from rapidfuzz import fuzz
from rapidfuzz import process as fuzz_process
from rapidfuzz.utils import default_process
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType
from pyspark.sql.functions import col, lit
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import ApplyMapping


def flatten_schema(schema: StructType, prefix: str = '') -> StructType:
    """Recursively iterate over a schema in a DataFrame, handling StructType nested elements,
    and return a flattened list of field names
    """
    flat_schema = []

    for field in schema:
        # Always add the field so that every level of nesting can be referenced
        flat_schema.append(StructField(f'{prefix}{field.name}', field.dataType, field.nullable))

        if isinstance(field.dataType, StructType):
            flat_schema += flatten_schema(field.dataType, prefix=f'{prefix}{field.name}.')

        if ( isinstance(field.dataType, ArrayType) and
                (isinstance(field.dataType.elementType, StructType)) ):
            flat_schema += flatten_schema(
                field.dataType.elementType,
                prefix=f'{prefix}{field.name}.'
            )

    return StructType(flat_schema)


def escape_field_name(name: str) -> str:
    """Escape Spark DataFrame field name with backticks if not already present
    """
    return '`' + name + '`' if '`' not in name else name


def unescape_field_name(name: str) -> str:
    """Remove backticks from Spark DataFrame field name
    """
    return name.replace('`', '')


def custommapping(df: DataFrame, field_mapping_list: list, args: dict, lineage, strict: bool = False) -> DataFrame:
    """Apply a custom field mapping to the data schema in a DataFrame
    Uses RapidFuzz for fuzzy matching: https://maxbachmann.github.io/RapidFuzz/

    Parameters
    ----------
    df
        Spark DataFrame on which to apply schema mapping
    field_mapping_list
        List of dictionary objects with the form:
            'sourcename': 'source_field_name',
            'destname': 'destination_field_name', use Null to drop a column
            'threshold': (optional) fuzzy match minimum confidence level
            'scorer': (optional, must be specified with threshhold) fuzzy match scoring algorithm
    args
        Glue job arguments, from which source_key and execution_id are used
    lineage
        Initialized lineage class object from the calling job
    strict
        If true, a field in the mapping that is missing from the schema will cause an error

    Returns
    -------
    DataFrame
        Spark DataFrame with the custom mapping applied
    """
    unmapped_fields = [ field.name for field in flatten_schema(df.schema) ]

    select_list = []
    for map_row in field_mapping_list:
        in_schema = True
        # Omit fuzzy matching rows from direct mapping
        if not map_row.get('threshold'):
            try:
                # Prepare unmapped_fields for fuzzy matching, and log message
                unmapped_fields.remove(unescape_field_name(map_row['sourcename']))
            except ValueError:
                in_schema = False

            # null mappings are explicit so check separately so we can remove them from unmapped
            if map_row['destname'].lower() != 'null' and (in_schema or strict):
                select_list.append(
                    col(escape_field_name(map_row['sourcename'])).alias(map_row['destname'])
                )

    # Perform fuzzy match with rapidfuzz, specified sort, and column alias
    if unmapped_fields:
        for map_row in field_mapping_list:
            if map_row.get('threshold'):
                match, score, _ = fuzz_process.extractOne(
                    map_row['sourcename'],
                    unmapped_fields,
                    processor=default_process,
                    scorer=getattr(fuzz, map_row['scorer'])
                )
                if score >= int(map_row['threshold']):
                    select_list.append(col(escape_field_name(match)).alias(map_row['destname']))
                    unmapped_fields.remove(match)
                    # Add match to mutable dictionary object to use for lineage
                    map_row['match'] = match

                print(f"Fuzzy matched {map_row['sourcename']} with column {match} and score {score}")

    if unmapped_fields:
        print(f'Discarded unmapped fields {unmapped_fields}')

    lineage.update_lineage(df, args['source_key'], 'mapping', map=field_mapping_list)
    return df.select(select_list)


def merge_catalog_schema(existing_schema: list, new_schema: list) -> list:
    """Merge two Glue Catalog schemas, keeping all existing columns and adding new ones

    Uses set operations following the pattern in check_schema_change().
    This prevents column loss when incremental batches have varying fields.

    Parameters
    ----------
    existing_schema
        Schema that already exists in the Glue Catalog
        List of Dict objects containing, at least, elements Name and Type
    new_schema
        Incoming (new) data file schema; same format as existing schema

    Returns
    -------
    list
        Merged schema containing all existing columns plus any new columns
    """
    existing_schema_map = { field_def['Name']: field_def for field_def in existing_schema }
    existing_schema_set = set(existing_schema_map.keys())
    new_schema_map = { field_def['Name']: field_def for field_def in new_schema }
    new_schema_set = set(new_schema_map.keys())

    added_fields = new_schema_set - existing_schema_set
    if added_fields:
        merged_schema = existing_schema + [ new_schema_map[name] for name in added_fields ]
        print(f'Permissive schema merge: added {added_fields}, '
            f'retained {len(existing_schema)} existing columns')
        return merged_schema
    else:
        print(f'Permissive schema merge: no new columns to add, '
            f'retaining {len(existing_schema)} existing columns')
        return existing_schema


def align_df_with_catalog_schema(df: DataFrame, catalog_schema: list, partition_keys: set) -> DataFrame:
    """Align DataFrame columns with Glue Catalog schema by adding missing columns as NULL

    When using permissive schema merge, the catalog accumulates columns from all batches.
    The current DataFrame may be missing columns from previous batches. This function adds
    those missing columns as NULL so that saveAsTable does not fail on column count mismatch.

    Parameters
    ----------
    df
        Spark DataFrame to align with the catalog schema
    catalog_schema
        List of Dict objects from upsert_catalog_table return value, containing Name and Type
    partition_keys
        Set of partition column names to exclude from alignment

    Returns
    -------
    DataFrame
        Spark DataFrame with missing columns added as NULL
    """
    catalog_column_names = { col_def['Name'] for col_def in catalog_schema
        if col_def['Name'] not in partition_keys }
    df_column_names = set(df.columns) - set(partition_keys)

    missing_columns = catalog_column_names - df_column_names
    if missing_columns:
        print(f'Aligning DataFrame with catalog schema: adding {missing_columns} as NULL')
        for col_name in missing_columns:
            df = df.withColumn(col_name, lit(None).cast('string'))

    return df


def custommapping_with_glue(dyf: DynamicFrame, field_mapping_list: list, args: dict, lineage) -> DynamicFrame:
    """Apply a custom field mapping to the data schema in a Glue DynamicFrame
    """
    # NOTE: Fuzzy matching mappings will be skipped
    prepared_map = [ (map_row['sourcename'], map_row['destname'])
        for map_row in field_mapping_list
        if not map_row['threshold'] and map_row['destname'].lower() != 'null' ]

    # NOTE: ApplyMapping.apply "optimizes" DecimalTypes, reorders fields, and re-samples columns
    #   resulting in Null/Void column types
    # NOTE: Discarded unmapped fields will not be logged
    lineage.update_lineage(dyf, args['source_key'], 'mapping', map=field_mapping_list)
    return ApplyMapping.apply(
        info='Field name mapping transform',
        frame=dyf,
        mappings=prepared_map,
        transformation_ctx=f"{args['execution_id']}-custommapping",
    )