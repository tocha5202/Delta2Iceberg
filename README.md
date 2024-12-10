# Delta2Iceberg
 
# Migration Deltalake to Iceberg using Spark

Migrating data from Delta Lake to Iceberg can be approached in several ways depending on your use case, performance requirements, and data processing needs.

## Here are some common methods for performing this migration:

### Using CTAS (Create Table As Select)  
**What is it?**  
This method creates a new table by selecting and transforming data from an existing table all at once.

**Pros:**  
- Simplicity: Directly creates the target table with one command.  
- Useful for large datasets where you want to reformulate the schema.

**Cons:**  
- Can be resource-intensive; may require more time for large datasets.

---

### Using DataFrames  
**What is it?**  
Load the Delta table into a DataFrame, apply any required transformations, and then write the DataFrame to an Iceberg table.

**Pros:**  
- Flexibility: You can break the migration into smaller steps and perform transformations as needed before writing.  
- Better for iterative processing and more control over data operations.

**Cons:**  
- Slightly more complex; requires more lines of code if you're doing several transformations.

---

### Using Deltalake to Iceberg Snapshot  
**What is it?**  
Snapshot directly creates a new Iceberg table based on the existing Delta Lake table’s metadata and the data files rather than rewriting the entire dataset.

**Pros:**  
- During a snapshot migration, Iceberg creates a new table based on the metadata of the existing Delta Lake table. This means that it references the existing data files in the Delta Lake location rather than duplicating the data immediately.  
- Since it leverages the metadata from the existing Delta Lake, it may minimize data movement, leading to quicker migration times. Supports features like time travel and schema evolution while copying the data.

**Cons:**  
- Because tables created by snapshotDeltaLakeTable are not the sole owners of their data files, they are prohibited from actions like `expire_snapshots` which would physically delete data files. Iceberg deletes, which only affect metadata, are still allowed.  
- Any operations which affect the original data files will disrupt the Snapshot's integrity.  
- DELETE statements executed against the original Delta Lake table will remove original data files and the `snapshotDeltaLakeTable` will no longer be able to access them.

More info under the following link:

https://iceberg.apache.org/docs/1.4.3/delta-lake-migration/

When you click on the 3x required modules, it will download the jars locally. The module 'iceberg-delta-lake' provides the interface for the snapshot. 
The iceberg-delta-lake module provides an interface named DeltaLakeToIcebergMigrationActionsProvider, which contains actions that helps converting from Delta Lake to Iceberg. The supported actions are: * snapshotDeltaLakeTable: snapshot an existing Delta Lake table to an Iceberg table
---

## Summary:
- CTAS is best for straightforward migrations of large tables where performance is critical, and transformations are minimal.  
- Using DataFrame allows for more granular control and processing, ideal for complex transformations or processing.  
- Using Deltalake to Iceberg Snapshot: If the goal is simply to migrate data while retaining its current format and schema, snapshotting is generally faster and more efficient.

---

## Conclusion:
All three methods effectively migrate data; the choice depends on your specific use case and performance requirements.
