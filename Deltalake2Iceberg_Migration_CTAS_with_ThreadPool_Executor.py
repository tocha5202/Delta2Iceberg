###
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Define a function to execute a CTAS statement and measure time
def execute_ctas(sql_command):
    start_time = time.time()  # Record start time
    success = True
    try:
        spark.sql(sql_command)  # Execute CTAS
    except Exception as e:
        success = False
        print(f"Error executing command: {sql_command}. Error: {e}")

    end_time = time.time()  # Record end time
    duration = end_time - start_time  # Calculate duration
    return (success, duration)  # Return success status and duration

# Create SQL commands for each CTAS operation
sql_commands = [
    """
    CREATE TABLE sys_iceberg_dev.sys_spark_iceberg.fraud_inventory USING ICEBERG AS 
    SELECT * FROM spark_catalog.sys_spark_deltalake.fraud_inventory
    """,
    """
    CREATE TABLE sys_iceberg_dev.sys_spark_iceberg.inventory USING ICEBERG AS 
    SELECT * FROM spark_catalog.sys_spark_deltalake.inventory
    """,
    """
    CREATE TABLE sys_iceberg_dev.sys_spark_iceberg.inventory_stats USING ICEBERG AS 
    SELECT * FROM spark_catalog.sys_spark_deltalake.inventory_stats
    """,
    """
    CREATE TABLE sys_iceberg_dev.sys_spark_iceberg.inventory_settings USING ICEBERG AS 
    SELECT * FROM spark_catalog.sys_spark_deltalake.inventory_setting
    """
]

# Initialize variables for tracking results
successful_ctas_count = 0
total_time = 0.0

# Use ThreadPoolExecutor to run CTAS commands in parallel
with ThreadPoolExecutor(max_workers=len(sql_commands)) as executor:
    future_to_command = {executor.submit(execute_ctas, cmd): cmd for cmd in sql_commands}
    
    for future in as_completed(future_to_command):
        success, duration = future.result()  # Get results
        
        if success:
            successful_ctas_count += 1
            print(f"Successfully executed: {future_to_command[future]} in {duration:.2f} seconds")
        else:
            print(f"Failed to execute: {future_to_command[future]}")
        
        total_time += duration

# Print summary of results
print(f"\nTotal successful CTAS operations: {successful_ctas_count}/{len(sql_commands)}")
print(f"Total time taken for all operations: {total_time:.2f} seconds")

















