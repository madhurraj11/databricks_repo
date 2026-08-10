# 🧠 Databricks & PySpark Notes

<p align="center">
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white" />
</p>

---

## 📑 Table of Contents

1. [Cluster & Spark Architecture](#-cluster--spark-architecture)
2. [Medallion Architecture](#-medallion-architecture)
3. [Optimize Cluster Performance and Cost](#-optimize-cluster-performance-and-cost)
4. [Batch Processing vs Structured Streaming](#-batch-processing-vs-structured-streaming)
5. [Data Cleaning](#-data-cleaning)
6. [Data Transformation](#-data-transformation)
7. [User-Defined Function (UDF)](#-user-defined-function-udf)
8. [Auto Loader / Streaming to Delta](#-auto-loader--streaming-to-delta)
9. [Auto-termination](#-auto-termination)
10. [Internal (Managed) vs External (Unmanaged) Tables](#-internal-managed-vs-external-unmanaged-tables)
11. [The Lakehouse](#-the-lakehouse)
12. [Delta Lake vs Lakehouse](#-delta-lake-vs-lakehouse)
13. [LakeBase](#-lakebase)
14. [Config Table](#-config-table)
15. [CDC](#-cdc)
16. [CDF (Change Data Feed)](#-cdf-change-data-feed)
17. [AQE](#-aqe)
18. [Unity Catalog](#-unity-catalog)
19. [Pool](#-pool)
20. [Auto-Scaling](#-auto-scaling)
21. [Data Lake vs Delta Lake](#-data-lake-vs-delta-lake)
22. [ACID](#-acid)
23. [Default Compression Technique](#-default-compression-technique)
24. [Delta Log](#-delta-log)
25. [Schema Evolution & Schema Enforcement](#-schema-evolution--schema-enforcement)
26. [Lazy Evaluation](#-lazy-evaluation)
27. [VACUUM](#-vacuum)
28. [Time Travel](#-time-travel)
29. [Wide vs Narrow Transformation](#-wide-vs-narrow-transformation)
30. [High vs Low Cardinality](#-high-vs-low-cardinality)
31. [Photon Engine](#-photon-engine)
32. [OOM Errors](#-oom-errors)
33. [Driver Node Failure in Prod](#-driver-node-failure-in-prod)
34. [Cache vs Persist](#-cache-vs-persist)
35. [Delta Live Tables](#-delta-live-tables)
36. [Delta Table SQL Q&A](#-delta-table-sql-qa)
37. [Convert JSON File into Delta File](#-convert-json-file-into-delta-file)
38. [Create Blank DataFrame](#-create-blank-dataframe)
39. [How to Deploy the Pipeline in ADF](#-how-to-deploy-the-pipeline-in-adf)
40. [A Day in the Life](#-a-day-in-the-life)
41. [Serverless Compute](#-serverless-compute)
42. [All-purpose vs Job Cluster](#-all-purpose-vs-job-cluster)
43. [Components of Databricks](#-components-of-databricks)
44. [TempView vs GlobalTempView](#-tempview-vs-globaltempview)
45. [ORDER BY vs SORT BY](#-order-by-vs-sort-by)
46. [Repartition vs Coalesce](#-repartition-vs-coalesce)
47. [Partitioning vs Bucketing](#-partitioning-vs-bucketing)
48. [Spark Query Optimization Techniques](#-spark-query-optimization-techniques)
49. [Constraints & Indexing on Delta Table](#-constraints--indexing-on-delta-table)
50. [Constraints in Delta Tables](#-constraints-in-delta-tables)
51. [Broadcast Join](#-broadcast-join)
52. [Data Skewness](#-data-skewness)
53. [Salting](#-salting)
54. [Z-Ordering and Implementation](#-z-ordering-and-implementation)
55. [Indexing in Delta Tables](#-indexing-in-delta-tables)
56. [Implementing Window Functions in a DataFrame](#-implementing-window-functions-in-a-dataframe)
57. [SCD Type 1 & Type 2](#-scd-type-1--type-2)

---

## 🧩 Cluster & Spark Architecture

Cluster is a group of computing resources to perform a task, if a cluster is having one driver and multiple worker node then it is called multinode cluster whereas if we have one node in the cluster then it is called as a single node cluster.

Spark follows a distributed, Driver-Worker architecture. It is designed to process massive amounts of data by breaking the work down into smaller tasks and distributing them across multiple machines in a cluster.

**1. The Core Components**
* **Driver Program (The Master):** This is the heart of a Spark application. It runs the main() function of your code and creates the SparkContext (or SparkSession). The Driver is responsible for converting your code into a logical execution plan (DAG) and coordinating the overall execution.
* **Cluster Manager:** This is the resource manager. When the Driver needs resources (CPU, memory) to run tasks, it asks the Cluster Manager. Common Cluster Managers include YARN (Hadoop), Kubernetes, Apache Mesos, or Spark's own Standalone Cluster Manager.
* **Executor (The Slaves):** Executors are responsible for executing the individual tasks assigned by the Driver and storing any cached data (in memory or on disk).

---

## 🥉 Medallion Architecture

**1. Bronze Layer (The Landing Zone)**
This is where the raw data lands exactly as it came from the source systems (e.g., via Azure Data Factory).

**2. Silver Layer (The Source of Truth)**
This is where apply your data engineering transformations (often using PySpark).

**3. Gold Layer (The Consumption Zone)**
This layer is built specifically for the business to use.

---

## 💰 Optimize Cluster Performance and Cost

The total cost of data bricks cluster depends on virtual machine cost and DBU cost.

1. Choose right cluster
2. Enable autoscaling
3. Enable autotermination
4. Use job cluster instead of all purpose cluster
5. Use photon engine/accelerator
6. Use delta lake instead of csv
7. Partition the large table
8. Use Z ordering while working on delta lake
9. Optimize small files
10. Use caching
11. Avoid select *
12. Use broadcast small table
13. Reduce shuffle operation
14. Optimise number of partitions
15. Use Adaptive query execution
16. Use spot instance(low priority)
17. Choose right virtual machine type
18. Use serverless compute once it is available
19. Monitor cluster utilization
20. Optimize spark code

---

## ⏱ Batch Processing vs Structured Streaming

**Batch Processing:** Processes bounded data (data with a known start and end) in large, discrete chunks at scheduled intervals (e.g., hourly, nightly). It is highly efficient for massive volumes, easier to debug, and much cheaper to run.

**Structured Streaming:** Processes unbounded data (an infinite stream) continuously as it arrives. It treats the live data stream as a table that is constantly being appended to, offering low-latency processing (seconds to milliseconds).

---

## 🧹 Data Cleaning

Data cleaning (or data cleansing) is the process of detecting and correcting inaccurate, incomplete, or corrupted records in a dataset. We are not changing the fundamental meaning of the data; we are just making sure it is accurate and usable.

---

## 🔄 Data Transformation

Data transformation is the process of changing the structure, format, or granularity of the data so it can be analyzed or consumed by downstream systems (like BI dashboards or machine learning models).

---

## 🐍 User-Defined Function (UDF)

User-Defined Function (UDF) allows you to write custom Python code and apply it row-by-row to a Spark DataFrame or SQL table.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 1. Define a standard Python function
def categorize_salary(salary):
    if salary > 100000:
        return "High"
    return "Standard"

# 2. Register it as a Spark UDF (You MUST specify the return type)
categorize_salary_udf = udf(categorize_salary, StringType())

# 3. Use it on a DataFrame
df = df.withColumn("Salary_Category", categorize_salary_udf(df["salary"]))
```

Register it as a global SQL function for this session
```python
spark.udf.register("Salary_Category", categorize_salary, StringType())
```

---

## 🚀 Auto Loader / Streaming to Delta

```python
# 1. Define your ADLS paths
source_data_path = "abfss://raw-data@yourstorage.dfs.core.windows.net/inbound_orders/"
checkpoint_path = "abfss://metadata@yourstorage.dfs.core.windows.net/checkpoints/orders/"
target_table = "bronze_orders"

# 2. Read stream using Auto Loader
df = (spark.readStream
  .format("cloudFiles")                                 # The magic keyword for Auto Loader
  .option("cloudFiles.format", "csv")                   # The underlying file format
  .option("cloudFiles.schemaLocation", checkpoint_path) # Where it saves the inferred schema
  .load(source_data_path))

# 3. Write stream to Delta Table
(df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)        # Tracks exactly which files are processed
  .option("mergeSchema", "true")                        # Allows the Delta table to accept new columns
  .trigger(availableNow=True)                           # Runs once to process new data, then stops (Batch mode)
  .toTable(target_table))
```

Auto Loader is an optimized ingestion mechanism that incrementally and automatically processes new data files as they arrive in cloud storage, loading them into Delta tables.

---

## ⏸ Auto-termination

Auto-termination is a cost-saving feature that automatically shuts down a cluster after a specified period of inactivity.

---

## 🗄 Internal (Managed) vs External (Unmanaged) Tables

**Internal (Managed) Tables**
When you create an Internal Table, Databricks manages both the metadata (the table name, column names, data types) and the actual data files.

**External (Unmanaged) Tables**
When you create an External Table, Databricks only manages the metadata. The actual data files live in your own cloud storage (like your Azure ADLS Gen2).

---

## 🏞 The Lakehouse

The Lakehouse is a modern data architecture that combines the flexibility of a Data Lake with a performance and reliability of a Data Warehouse into a single platform.

Lakehouse supports such type of data like csv, json, xml, parquet, delta, music, video. A Lakehouse performs faster performance because it uses metadata, file statistics, data skipping, and caching, instead of having to do full file directory scans like a traditional Data Lake.

**Components of Lakehouse:**
* BI tools
* ADLS Gen2
* Databricks
* Spark
* Delta Table

---

## 🔺 Delta Lake vs Lakehouse

Is delta lake is same as Lakehouse?
Delta lake is a storage framework whereas Lakehouse is a data architecture. Delta lake is a layer of lakehouse.

---

## 🧱 LakeBase

LakeBase is made on top of Lakehouse and this brings the feature of database like insert, update, delete.

---

## ⚙️ Config Table

Config Table (Configuration Table) is a small database table used to store metadata, settings, and parameters that control how your code or pipelines behave.

---

## 🔁 CDC

CDC is a design pattern used to identify and track data that has changed (inserts, updates, or deletes) in a database so that action can be taken using that changed data.

---

## 📋 CDF (Change Data Feed)

CDF (Change Data Feed) is a specific Delta Lake feature that tracks changes between the tables inside your Lakehouse (e.g., from Silver to Gold layers).
When you enable CDF on a Delta table, Databricks starts secretly recording every row-level change made to that table, along with metadata about what kind of change it was.

---

## 🤖 AQE

AQE dynamically coalesces shuffle partitions and optimizes joins at runtime based on actual data sizes, drastically reducing memory spikes.

---

## 🔐 Unity Catalog

Unity Catalog is the centralized, unified governance solution for all data and AI assets across your entire Lakehouse.

---

## 🏊 Pool

**What is a "Pool"?**
A Pool is a set of idle, ready-to-use virtual machines (VMs) kept running in the background.
Why do we use them? When you request a cluster to start up or autoscale, it usually takes several minutes for the cloud provider to provision new VMs. If you attach your cluster to the Pool, the cluster grabs the already-running VMs from the pool. This reduces cluster start-up and autoscaling times from minutes to mere seconds.

---

## 📈 Auto-Scaling

**What do you understand by Auto-Scaling? Can I change the max range later?**
Auto-scaling allows a cluster to automatically add or remove worker nodes based on the current workload. Yes, you can edit the cluster configuration at any time to change the min (e.g., 1) and max (e.g., from 4 to 6) node limits.

---

## 💧 Data Lake vs Delta Lake

Difference between Data Lake and Delta Lake:
A Data Lake (like ADLS) is simply a scalable storage repository for raw data. Delta Lake is an open-source storage layer that sits on top of the Data Lake. It brings ACID transactions, time travel (data versioning), and schema enforcement to Parquet files and vacuum.

---

## ✅ ACID

* **Atomicity:** It means all the transactions succeeded or failed, there is no such thing as a partially completed transaction.
* **Consistency:** It means transaction can only bring the database from one valid state to another valid state.
* **Isolation:** It means when multiple transactions occurred at the exact same time, they do not interfere with each other.
* **Durability:** It means once the transaction has successfully completed, its changes are permanent.

---

## 📦 Default Compression Technique

What is default compression technique in delta table?
Snappy

---

## 📜 Delta Log

What is delta log and its file format?
Delta log is a transaction log of delta file and it records all operations and changes made to the table in sequential order.

---

## 🧬 Schema Evolution & Schema Enforcement

**Schema Evolution:**
It's a Delta Lake feature that automatically accommodates data structure changes over time. If a new column arrives in the source data, mergeSchema=True allows Delta to seamlessly add that column to the existing table without breaking pipelines.

**Schema Enforcement:** It automatically rejects the write operation if the schema of the source data doesn't match the target table schema.

---

## 😴 Lazy Evaluation

Spark doesn't execute transformations (like .filter() or .select()) immediately. It builds a logical execution plan (a DAG). The execution only happens when an action (like .show(), .count(), or .write()) is called.

---

## 🧹 VACUUM

VACUUM is a data maintenance command that permanently deletes obsolete physical data files (usually Parquet files) from a storage directory. It specifically targets files that are no longer referenced by a Delta table's active transaction log and are older than a specified retention threshold (the default is 7 days).

---

## 🕰 Time Travel

Time Travel is the ability to query historical versions of a table based on a specific timestamp or version number. It allows you to see exactly what the data looked like at a given moment in the past, before recent updates or deletions occurred.

---

## ↔️ Wide vs Narrow Transformation

Wide vs Narrow Transformation:
* **Narrow:** Data required to compute the records in a single partition reside in at most one partition of the parent dataset (e.g., filter(), map()). No data shuffling over the network.
* **Wide:** Data required resides in multiple partitions, requiring a network shuffle (e.g., groupBy(), join()).

---

## 🔢 High vs Low Cardinality

What is high cardinality and low cardinality?
* **High Cardinality:** A column containing mostly unique values (e.g., employee_id, email addresses, bank account numbers).
* **Low Cardinality:** A column with very few distinct values relative to the size of the dataset (e.g., gender, boolean flags, or a status column with only 'Active', 'Pending', 'Closed').

---

## 🐆 Photon Engine

Photon is a specialized query execution engine created by Databricks. Unlike standard Spark—which runs on the Java Virtual Machine (JVM)—Photon is written entirely in C++ from the ground up. It is designed to maximize the hardware efficiency of modern cloud virtual machines. By enabling Photon on a Databricks cluster, you can drastically speed up Spark SQL queries, heavy joins, and aggregations without having to change a single line of your existing code.

---

## 🔥 OOM Errors

When a Spark job fails with an OOM error, I don't just increase cluster memory right away. First, I check the Spark UI to see exactly where it crashed: the Driver or an Executor.
* **If it is a Driver OOM:** This usually means I pulled too much data into the master node. I will look for rogue .collect() statements in the code and replace them, or check if I am trying to broadcast a table that is too large.
* **If it is an Executor OOM:** This is almost always caused by Data Skew, where one worker gets crushed by too much data. I fix this by salting the skewed keys to distribute the data evenly, or by forcing a Broadcast Hash Join.

---

## 🧯 Driver Node Failure in Prod

If driver node fails during prod, how can handle?
Unlike Executor failures, Spark cannot natively recover if the Driver node crashes because the Driver is the application. To handle this in production, you must run your jobs in Cluster Mode (using flags like --supervise or relying on Databricks Job Retries) so the cluster manager automatically provisions a brand new Driver if the original one dies. However, to prevent data loss or duplication when that new Driver wakes up, your PySpark code must utilize Checkpointing—especially in streaming jobs—so the new application can read the transaction log on your storage account and resume exactly where the crashed Driver left off.

---

## 🧠 Cache vs Persist

**cache():** If you are using the same intermediate DataFrame multiple times in a script, cache it in memory (df.cache()) so Spark doesn't recalculate it from scratch every time. When you call .cache() on a DataFrame, Spark saves it using the default storage level (which is MEMORY_AND_DISK).

**persist():** This is the customizable version of cache(). It allows you to specify exactly how you want the data stored by passing a Storage Level. For example, you can tell Spark to store it as MEMORY_ONLY, DISK_ONLY, or even MEMORY_AND_DISK_SER (which serializes the data to save space).

---

## 🛠 Delta Live Tables

Delta Live Tables is a declarative ETL framework for building reliable, maintainable, and testable data processing pipelines. Instead of manually writing boilerplate code to manage Spark clusters, handle task dependencies, and monitor data quality, DLT allows data engineers to just define what they want the data to look like using simple SQL or Python. Databricks then automatically manages the infrastructure, orchestration, and error handling behind the scenes.

---

## 📘 Delta Table SQL Q&A

**Can you create a blank delta table with four columns with partition with date column and location**
```sql
CREATE TABLE employee_partitioned (
    id INT,
    name STRING,
    date_col DATE,
    location STRING
)
USING DELTA
PARTITIONED BY (date_col, location);
location 'abfss://finalcleandata@skpb30adls.dfs.core.windows.net/deltab31'
```

**In this delta table insert one record**
```sql
INSERT INTO employee_partitioned VALUES (1, 'Test Name', '2026-06-28', 'Delhi');
```

**Convert the id column into string in this delta table**
Delta Lake requires schema overwriting for data type changes:
```python
df = spark.read.format("delta").load('employee_partitioned')
df.withColumn("id", col("id").cast("string"))

df.write.format("delta").mode("overwrite").save("employee_partitioned")
```

**How to check version of a delta table**
```sql
DESCRIBE HISTORY employee_partitioned;
```

**I want to display the version 0**
```python
spark.read.format("delta").option("versionAsOf", 0).table("employee_partitioned").show()
```
```sql
Select * from delta.`/Volumes/workspace/default/madhurvolume/delta` version as of 0
```

**How to find the difference of two versions of a delta table**
```sql
select * from delta.`/Volumes/workspace/default/madhurvolume/delta`  version as of 2
except all
select * from delta.`/Volumes/workspace/default/madhurvolume/delta`  version as of 3
```

**1. EXCEPT (or subtract() in PySpark)**
It returns a new DataFrame containing only the unique rows from the first dataset that do not exist in the second dataset.

**2. ExceptAll() (or EXCEPT ALL in SQL)**
It returns a new DataFrame containing all rows from the first dataset that do not exist in the second dataset, preserving duplicate rows.

---

## 🔄 Convert JSON File into Delta File

```python
from pyspark.sql.functions import col, explode

df_raw = spark.read.option("multiline", "true").json("/Volumes/workspace/default/madhurvolume/sample_nested_1000_records.json")

df_exploded_orders = df_raw.withColumn("order", explode(col("orders")))
df_exploded_items = df_exploded_orders.withColumn("item", explode(col("order.items")))

df_flat = df_exploded_items.select(
    col("user_id"),
    col("name"),
    col("email"),
    col("created_at"),
    col("address.street").alias("street"),
    col("address.city").alias("city"),
    col("address.state").alias("state"),
    col("address.zip").alias("zipcode"),
    col("order.order_id").alias("order_id"),
    col("order.amount").alias("order_amount"),
    col("item.product_id").alias("product_id"),  
    col("item.quantity").alias("quantity")       
)

display(df_flat)
df_flat.write.format("delta").mode("overwrite").save('/Volumes/workspace/default/madhurvolume/json_to_delta')
```

---

## 🆕 Create Blank DataFrame

**How to create blank data frame**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Define the schema with four columns
schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True),
    StructField("col3", StringType(), True),
    StructField("col4", IntegerType(), True) 
])

# 2. Create the empty DataFrame by passing an empty list and the schema
df = spark.createDataFrame([], schema)

# 3. Display the schema to verify the changes
df.printSchema()

# Optional: Display the empty DataFrame in Databricks
display(df)
```

---

## 🚢 How to Deploy the Pipeline in ADF

How to deploy the pipeline in ADF?
**Develop & Merge:** I build my pipeline in a feature branch in the Dev environment. Once it is tested and reviewed, I merge it into the main branch.
**Publish:** I click "Publish" in the Dev Data Factory. This automatically converts the code into an ARM template.
**Automated Deployment:** A CI/CD release pipeline (like Azure DevOps) grabs that ARM template and pushes it to QA, and then to Production.
**Update Parameters:** During that deployment, the pipeline automatically swaps out the Dev database connections and passwords for the real Production ones.

---

## 🗓 A Day in the Life

**1. Morning (Monitor & Plan):** The first thing I check my mails and I log into Azure Data Factory or Databricks. I do is check the health of production pipelines to see if any scheduled overnight batch jobs failed, timed out, or had data quality issues. If a pipeline failed, investigate the logs, identify the root cause, and fix it or alert the team. And also I checking pending tasks and which task is on priority for today then it will doing first.

**2. Mid-Day (Core Development):** I look at slow-running jobs and optimize them. This might mean fixing data skew, adjusting Spark cluster sizes, or optimizing SQL queries so they run faster and cost less.

**3. Afternoon (Review & Deploy):** I review code written by our teammates. I check if their Databricks notebooks and ADF pipelines are efficient and follow best practices before they merge them into the main branch. I attend the evening Agile/Scrum meeting to update the team on what I did today, what I will be doing tomorrow, and if I have any blockers.

**4. End of Day (Wrap-Up):** Finally, I update our technical documentation and make sure all my work is committed to Git before logging off.

---

## ☁️ Serverless Compute

Serverless compute is a cloud computing model where the cloud provider dynamically manages the allocation and provisioning of cluster. As a developer, I don't have to provision, scale, or maintain any virtual machines; I simply provide the code or query, and the platform executes it.

---

## ⚖️ All-purpose vs Job Cluster

| Feature | All-purpose / Interactive Cluster | Job Cluster |
|---|---|---|
| Primary Use | Interactive development, ad-hoc analysis, notebooks | Automated job execution (ETL pipelines, scheduled workflows) |
| Cluster Lifecycle | Long-running (manually started/stopped) | Ephemeral (created at job start, terminated after completion) |
| Cost Efficiency | Higher cost (idle time may incur charges) | Cost-optimized (runs only during job execution) |
| Startup Time | Slower initially, but reused once running | Startup time per job execution |
| Concurrency | Supports multiple users and notebooks simultaneously | Typically dedicated to a single job run |
| State Management | Maintains state (cached data, variables persist) | Stateless (fresh environment for each run) |
| Reliability | Lower for production (manual interference possible) | High (isolated, reproducible runs) |
| Best Use Case | Development, testing, debugging, exploration | Production pipelines, batch jobs, scheduled workloads |
| Auto Termination | Configurable (e.g., terminate after idle time) | Automatic termination after job completion |
| Example | Running notebooks interactively in Databricks workspace | Running scheduled ETL job via Databricks Jobs |

---

## 🧱 Components of Databricks

What are the Components of Databricks?
* Workspace
* Clusters
* Jobs
* Notebooks
* Table
* DBFS/Volume

---

## 🔎 TempView vs GlobalTempView

| Feature | TempView | GlobalTempView |
|---|---|---|
| Scope | Session-level (only within the current Spark session) | Application-level (accessible across multiple sessions) |
| Visibility | Accessible only in the same notebook/session | Accessible across notebooks within the same Spark application |
| Namespace | No database prefix required | Must use global_temp database prefix |
| Lifetime | Ends when the session terminates | Ends when the Spark application stops |
| Creation Method | createOrReplaceTempView() | createOrReplaceGlobalTempView() |
| Access Syntax | spark.sql("SELECT * FROM temp_view") | spark.sql("SELECT * FROM global_temp.global_view") |
| Use Case | Temporary transformations within a single session | Sharing data across multiple notebooks/sessions |

---

## ↕️ ORDER BY vs SORT BY

| Feature | ORDER BY | SORT BY |
|---|---|---|
| Scope of Ordering | Applies globally across all partitions → ensures complete ordering of the output. | Applies locally within each partition → results may not be globally ordered. |
| Performance | More expensive, as Spark performs full shuffle across partitions. | More efficient, avoids full shuffle; sorts only within partitions. |
| Use Case | When fully sorted, deterministic output is required (e.g., reports, exports). | When partial ordering is sufficient (e.g., exploratory queries, performance optimization). |
| Compatibility | Cannot be combined with SORT BY, CLUSTER BY, or DISTRIBUTE BY. | Can be used with partitioning strategies, but does not guarantee global order. |
| Default Direction | Ascending order by default (ASC), can specify DESC. | Ascending order by default (ASC), can specify DESC. |

---

## 🔀 Repartition vs Coalesce

**1. Data Distribution: Repartition vs. Coalesce**
Both methods change the number of partitions in your Spark DataFrame, but they do it very differently under the hood.
* **repartition(n):** repartition is used to increase or decrease the number of partitions. It performs a full shuffle of your data across the cluster's network. It completely tears down the existing partitions and redistributes the data evenly across the new number of partitions you specify.
* **coalesce(n):** coalesce is used to decrease the number of partitions in a DataFrame. It avoids a full "shuffle" of data across the network. Instead of moving data between nodes, it simply merges existing partitions on the same worker node.

---

## 📂 Partitioning vs Bucketing

**2. Storage Strategy: Partitioning vs. Bucketing**
Both techniques control how data is physically saved to disk, aiming to speed up future queries by skipping irrelevant files.
* **Partitioning:** Organizes data into folder hierarchies based on a column (e.g., year=2026/month=05/).
    * Best for: Low-cardinality columns (columns with a few distinct values, like dates, countries, or statuses).
    * Risk: Partitioning on a high-cardinality column (like user_id) creates millions of tiny files, which will crash your cluster (the "small file problem").
* **Bucketing:** Divides data into a fixed number of files (buckets) based on a mathematical hash of a column.
    * Best for: High-cardinality columns that are frequently used in JOIN or GROUP BY operations (e.g., user_id, transaction_id). It pre-shuffles the data on disk, making downstream joins incredibly fast.

---

## ⚡ Spark Query Optimization Techniques

**3. Spark Query Optimization Techniques**
When a Spark job is running slow, these are the primary levers you pull:
* **Predicate Pushdown:** Filter your data (WHERE clauses) as early as possible so you load less data into memory.
* **Broadcast Joins:** (Explained below).
* **Caching/Persisting:** If you are using the same intermediate DataFrame multiple times in a script, cache it in memory (df.cache()) so Spark doesn't recalculate it from scratch every time.
* **Handling Data Skew:** (Explained below).
* **Optimized File Formats:** Always read/write in columnar formats like Parquet or Delta, never CSV or JSON for heavy processing.
* **Adaptive Query Execution (AQE):** Ensure AQE is enabled (default in Spark 3+). It dynamically optimizes the execution plan while the job is running (e.g., automatically combining small partitions).

---

## 🔐 Constraints & Indexing on Delta Table

Constraints & Indexing on Delta Table:
* **Constraints:** Delta supports NOT NULL and CHECK constraints.
* **Indexing:** Delta doesn't use traditional B-Tree indexes. It uses Z-Ordering (colocating related information in the same set of files) and Liquid Clustering.
* **Optimization:** Run OPTIMIZE table_name ZORDER BY (column_name).

---

## ✅ Constraints in Delta Tables

**4. Constraints in Delta Tables**
Unlike standard Parquet files, Delta Lake allows you to enforce data quality directly at the storage layer, preventing bad data from entering your tables.
* **NOT NULL Constraints:** Ensures a specific column cannot contain null values.
* **CHECK Constraints:** Enforces specific boolean logic on a row before it is written.
    * Example: ALTER TABLE employees ADD CONSTRAINT valid_age CHECK (age > 18);

---

## 🔗 Broadcast Join

**5. Broadcast Join**
In a standard join, Spark has to shuffle data from both tables across the network so matching keys end up on the same node. This is slow.
A Broadcast Join is used when you join a massive table with a very small table (like a lookup table). Instead of shuffling both tables, Spark copies (broadcasts) the entire small table to the memory of every single worker node. The massive table stays put, and the join happens locally on each node without any network shuffling.

---

## ⚖️ Data Skewness

**6. Data Skewness**
Data skew occurs when your data is unevenly distributed across your cluster's partitions.
Imagine you are partitioning sales data by city. If 90% of your sales happen in "New York" and 10% in other cities, the node processing the "New York" partition gets slammed with data while the other nodes finish in seconds and sit idle. The job is bottlenecked by that single "straggler" node, leading to massive delays or Out-Of-Memory (OOM) errors.

---

## 🧂 Salting

**7. Salting**
Salting is the classic engineering technique used to fix data skew during joins or aggregations.
If the "New York" key is causing skew, you "salt" the key by appending a random number to it (e.g., New York_1, New York_2, New York_3). This artificially breaks the massive chunk of data into smaller, distinct keys, distributing the workload evenly across multiple worker nodes.

---

## 🧭 Z-Ordering and Implementation

**8. Z-Ordering and Implementation**
Z-Ordering is a technique used in Delta Lake to co-locate related information in the same set of files. It is an advanced version of sorting.
If you frequently query a massive table using multiple high-cardinality columns (e.g., querying by user_id AND product_id), standard sorting only helps with the first column. Z-Ordering maps multidimensional data into a single dimension, allowing the engine to mathematically skip over thousands of files that don't contain your target data.

**Implementation in SQL/Databricks:**
```sql
OPTIMIZE table_name ZORDER BY (user_id, product_id);
```

---

## 🗂 Indexing in Delta Tables

**9. Indexing in Delta Tables**
Delta Lake does not use traditional B-Tree indexes like a relational database (SQL Server/Oracle). Instead, it relies on metadata and file-skipping techniques:
* **Min/Max Statistics:** Delta automatically records the minimum and maximum values for columns inside each Parquet file's metadata. If a query looks for age = 30, and a file's stats say min: 40, max: 60, Delta skips reading that file entirely.
* **Z-Ordering:** (As mentioned above, this groups data tightly so Min/Max stats are highly effective).
* **Bloom Filters:** A probabilistic data structure you can enable on a Delta table. It creates an index that quickly answers if a specific value (like a specific transaction_id) might be in a file, or is definitely not in a file.

```sql
CREATE BLOOMFILTER INDEX ON TABLE events FOR COLUMNS(user_id); 

OPTIMIZE events;
```

---

## 🪟 Implementing Window Functions in a DataFrame

**10. Implementing Window Functions in a DataFrame**
Window functions allow you to perform calculations across a set of rows related to the current row, without collapsing the output (unlike a GROUP BY, which reduces the row count).
To implement this in PySpark, you define a WindowSpec (how to group and order the data) and apply an aggregation or ranking function over that window.

**Example: Finding the highest earner in each department:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank

# 1. Define the Window Specification
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# 2. Apply a function over the window
df_ranked = df.withColumn("salary_rank", rank().over(window_spec))

# 3. Filter for the top earners
top_earners = df_ranked.filter(col("salary_rank") == 1)
```

---

## 🔁 SCD Type 1 & Type 2

```python
def scd_type1(src_df,tgt_df):
    joined_df = tgt_df.alias("tgt").join(
        src_df.alias("src"),
        on=(col("src.emp_id") == col("tgt.emp_id")),
        how="full_outer"
    )

    is_newer_or_new = (col("src.last_updated") > col("tgt.last_updated")) | col("tgt.emp_id").isNull()

    final_df = joined_df.select(
        "emp_id",
        when(is_newer_or_new, col("src.name")).otherwise(col("tgt.name")).alias("name"),
        when(is_newer_or_new, col("src.role")).otherwise(col("tgt.role")).alias("role"),
        when(is_newer_or_new, col("src.last_updated")).otherwise(col("tgt.last_updated")).alias("last_updated")
    )
    return final_df
```

```python
from pyspark.sql.functions import col, lit, current_date, coalesce, when

def apply_scd_type_2(src_df, tgt_df):
    
    active_target = tgt_df.filter(col("is_active") == True)
    inactive_target = tgt_df.filter(col("is_active") == False)

    joined_df = src_df.alias("src").join(
        active_target.alias("tgt"),
        on=(col("src.emp_id") == col("tgt.emp_id")),
        how="full_outer"
    )

    new_records = joined_df.filter(col("tgt.emp_id").isNull()) \
        .select(col("src.emp_id"), col("src.name"), col("src.role"), col("src.last_updated")) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit("9999-12-31")) \
        .withColumn("is_active", lit(True))

    changed_condition = col("src.last_updated") > col("tgt.last_updated")

    updated_new_version = joined_df.filter(col("src.emp_id").isNotNull() & col("tgt.emp_id").isNotNull() & changed_condition) \
        .select(col("src.emp_id"), col("src.name"), col("src.role"), col("src.last_updated")) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit("9999-12-31")) \
        .withColumn("is_active", lit(True))

    updated_old_version = joined_df.filter(col("src.emp_id").isNotNull() & col("tgt.emp_id").isNotNull() & changed_condition) \
        .select(col("tgt.emp_id"), col("tgt.name"), col("tgt.role"), col("tgt.last_updated"), col("tgt.start_date")) \
        .withColumn("end_date", current_date()) \
        .withColumn("is_active", lit(False))

    unchanged_records = joined_df.filter(
        col("src.emp_id").isNull() | 
        (col("src.emp_id").isNotNull() & col("tgt.emp_id").isNotNull() & ~changed_condition)
    ).select(
        col("tgt.emp_id"), col("tgt.name"), col("tgt.role"), col("tgt.last_updated"),
        col("tgt.start_date"), col("tgt.end_date"), col("tgt.is_active")
    )

    final_target_df = new_records \
        .unionByName(updated_new_version) \
        .unionByName(updated_old_version) \
        .unionByName(unchanged_records) \
        .unionByName(inactive_target)

    return final_target_df
```
