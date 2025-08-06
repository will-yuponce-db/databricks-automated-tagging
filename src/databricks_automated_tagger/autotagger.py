# Function to join two tables, write the result, and propagate metadata tags
def join_and_write_with_metadata(table_name1, table_name2, tag_rules, on, how, save_as):
    # Read both tables as DataFrames
    df1 = spark.sql(f"SELECT * FROM {table_name1}")
    df2 = spark.sql(f"SELECT * FROM {table_name2}")
    # Get table tags for both tables from information_schema
    table_tag_df = spark.sql(
        f"SELECT * FROM {save_as.split('.')[0]}.information_schema.table_tags "
        f"WHERE table_name = '{table_name1.split('.')[-1]}' OR table_name = '{table_name2.split('.')[-1]}'"
    ).collect()
    # Determine which DataFrame has more columns
    biggest_df, smallest_df = (
        (df1, df2) if len(df1.columns) > len(df2.columns) else (df2, df1)
    )
    # Find columns in the smaller DataFrame that are not in the bigger one
    col_dif = [col for col in smallest_df.columns if col not in biggest_df.columns]
    # Join the DataFrames on the specified column
    joined_df = biggest_df.join(smallest_df.select(col_dif + [on]), on=on, how=how)
    # Write the joined DataFrame as a table
    joined_df.write.mode("overwrite").option("overWriteSchema", "true").saveAsTable(
        save_as
    )
    # Apply table-level tags based on tag_rules and existing tags
    for tag_rule in tag_rules:
        key = list(tag_rule.keys())[0]
        tag_dict = tag_rule[key]
        highest_priority_tag = list(tag_dict.keys())[0]
        for row in table_tag_df:
            if row["tag_name"] == key:
                highest_priority_tag = (
                    row["tag_value"]
                    if tag_dict[row["tag_value"]] > tag_dict[highest_priority_tag]
                    else highest_priority_tag
                )
        if highest_priority_tag != "":
            spark.sql(f"SET TAG ON TABLE {save_as} {key} = {highest_priority_tag}")
    # Get column tags for both tables from information_schema
    column_tag_df = spark.sql(
        f"SELECT * FROM {table_name1.split('.')[0]}.information_schema.column_tags WHERE table_name = '{table_name1.split('.')[-1]}' OR table_name = '{table_name2.split('.')[-1]}' AND tag_name = 'entity_type'"
    ).collect()
    column_tags = {}
    # Organize column tags by column name
    for row in column_tag_df:
        if row.column_name not in column_tags:
            column_tags[row.column_name] = {}
            column_tags[row.column_name][row.tag_name] = row.tag_value
    # Set column tags on the joined table
    for column_name in column_tags:
        for tag_name in column_tags[column_name]:
            spark.sql(
                f"SET TAG ON COLUMN {save_as + '.' + column_name} {tag_name} = {column_tags[column_name][tag_name]}"
            )

# Function to apply entity type tags to columns of a table
def apply_column_tags(table_name):
    # Read a sample of the table
    df = spark.sql(f"SELECT * FROM {table_name} LIMIT 10")
    # For each column, create a new column with the detected entity label
    for column in df.columns:
        df = df.withColumn(f"{column}_entity_type", get_entity_label(df[column]))
    entity_cols = [c for c in df.columns if c.endswith("_entity_type")]

    # Aggregate to get the most frequent label for each column
    result = []
    for c in entity_cols:
        mode_df = (
            df.groupBy(c)
            .agg(count("*").alias("cnt"))
            .orderBy(desc("cnt"))
            .select(first(c).alias(c))
            .limit(1)
        )
        result.append(mode_df)

    # Combine the results for all columns
    agg_df = reduce(lambda a, b: a.crossJoin(b), result)
    agg_row = agg_df.collect()[0]
    # Set the entity_type tag for each column if an entity is found
    for item in agg_row.asDict().items():
        if item[1] != "No entities found":
            spark.sql(
                f"SET TAG ON COLUMN {table_name + '.' + '_'.join(item[0].split('_')[0:-2])} entity_type = {item[1]}"
            )

# Function to apply table-level tags based on column entity tags and rules
def apply_table_tags(table_name, table_rules):
    # Get all entity_type tags for columns in the table
    tag_df = spark.sql(
        f"SELECT * FROM {table_name.split('.')[0]}.information_schema.column_tags"
        f" WHERE table_name = '{table_name.split('.')[-1]}' AND tag_name = 'entity_type'"
    ).collect()
    entity_tags = [row.tag_value for row in tag_df]
    # Apply each rule to determine if the table should be tagged
    for rule in table_rules:
        cond_arr = [entity in rule["key_words"] for entity in entity_tags]
        if rule["operation"] == "or" and any(cond_arr):
            spark.sql(f"SET TAG ON TABLE {table_name} {rule['tag_key']} = {rule['tag_value']}")
        if rule["operation"] == "and" and all(cond_arr):
            spark.sql(f"SET TAG ON TABLE {table_name} {rule['tag_key']} = {rule['tag_value']}")

# Function to apply column and table tags to all tables in a catalog
def apply_tagging_to_catalog(catalog, table_rules):
  # Get all schemas in the catalog
  schema_df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
  for schema_row in schema_df.collect():
    schema = schema_row["databaseName"]
    if schema != "information_schema":
      # Get all tables in the schema
      table_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
      for table_row in table_df.collect():
        try:
            # Apply column and table tags to each table
            apply_column_tags(f"{catalog}.{schema}.{table_row['tableName']}")
            apply_table_tags(f"{catalog}.{schema}.{table_row['tableName']}", table_rules)
        except:
            print(f"Could not apply tags to table {catalog}.{schema}.{table_row['tableName']}")