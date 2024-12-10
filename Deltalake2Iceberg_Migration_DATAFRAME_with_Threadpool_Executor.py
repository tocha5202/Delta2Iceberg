from concurrent.futures import ThreadPoolExecutor, as_completed
import time

s3_paths = [
    "s3a://deltalake-sysdatalakehouse-us-east-1/dev/sys_raw_db.20240923/inventory_stats/",
    "s3a://deltalake-sysdatalakehouse-us-east-1/dev/sys_raw_db.20240923/inventory/",
    "s3a://deltalake-sysdatalakehouse-us-east-1/dev/sys_raw_db.20240923/fraud_inventory/",
    "s3a://deltalake-sysdatalakehouse-us-east-1/dev/sys_raw_db.20240923/inventory_settings/"
]

# Function to handle the migration of each Delta table to Iceberg
def migrate_table(s3_path):
    # Extract the table name from the S3 path
    table_name = s3_path.split("/")[-2].split(".")[-1]  # Getting the base name of the table

    try:
        # Step 1: Load the Delta DataFrame
        print(f"Loading Delta table from {s3_path}")
        start_time = time.time()  # Start timing

        delta_df = spark.read.format("delta").load(s3_path)

        # Step 2: Get the schema and construct the CREATE TABLE statement for Iceberg
        schema_fields = delta_df.schema.fields
        create_table_sql = f"CREATE TABLE sys_iceberg_dev.sys_spark_iceberg.{table_name} ("

        # Construct the column definitions
        column_definitions = []
        for field in schema_fields:
            column_name = field.name
            column_type = field.dataType.simpleString()
            # Simply use the original type without casting
            column_definitions.append(f"{column_name} {column_type}")

        # Finalize the CREATE TABLE SQL statement
        create_table_sql += ", ".join(column_definitions) + ") USING iceberg"
        
        # Execute the CREATE TABLE command
        print(f"Creating Iceberg table: {create_table_sql}")
        spark.sql(create_table_sql)

        # Step 3: Write DataFrame to the Iceberg table
        iceberg_table_name = f"sys_iceberg_dev.sys_spark_iceberg.{table_name}"
        delta_df.write.format("iceberg").mode("overwrite").save(iceberg_table_name)

        duration = time.time() - start_time  # End timing
        print(f"Data ingested from Delta table '{s3_path}' to Iceberg table '{iceberg_table_name}' in {duration:.2f} seconds.")
        return duration  # Return the duration for total time calculation

    except Exception as e:
        print(f"Failed to migrate table from {s3_path}. Error: {e}")
        return 0  # Return 0 if failed

# Measure the overall migration time
overall_start_time = time.time()

# Use ThreadPoolExecutor to run migrations in parallel
with ThreadPoolExecutor(max_workers=len(s3_paths)) as executor:
    futures = {executor.submit(migrate_table, path): path for path in s3_paths}

    # Wait for all futures to complete
    total_time = 0
    for future in as_completed(futures):
        total_time += future.result()  # Accumulate the duration of each migration

# Calculate total time taken for all migrations
overall_duration = time.time() - overall_start_time
print(f"\nTotal migration time for all tables: {overall_duration:.2f} seconds.")






























