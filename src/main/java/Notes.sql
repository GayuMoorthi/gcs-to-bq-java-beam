--how to ensure incremental data loaded in bigquery and dataflow

--To ensure incremental data loading in BigQuery and Dataflow, you need to combine watermark-based filtering in Dataflow with MERGE/upsert patterns and partitioning in BigQuery. Here's the complete approach:

--Core Incremental Load Pattern
/* Step 1: Watermark Column in Source
Identify a watermark column in your source table (typically updated_at, last_modified, or created_at): 

| Attribute   | Recommendation                     |
| ----------- | ---------------------------------- |
| Column type | TIMESTAMP or DATETIME              |
| Must be     | Always populated, never NULL       |
| Purpose     | Track when rows were added/updated |  */

--Step 2: Dataflow - Extract Only New/Updated Records
--In Dataflow (Apache Beam Python), filter data using the watermark:

/* import apache_beam as beam

# Track last watermark (store in Datastore/Metadata table)
last_watermark = get_last_watermark()  # e.g., from metadata table

with beam.Pipeline(options=pipeline_options) as p:
    (p 
     | "Read from Source" >> beam.io.ReadFromBigQuery(
         query=f"""
         SELECT * FROM source_table 
         WHERE updated_at > TIMESTAMP('{last_watermark}')
         """
     )
     | "Write to Staging" >> beam.io.WriteToBigQuery(
         "project:dataset.staging_table",
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
     )
    )
For streaming, use windowing + watermarks to handle late-arriving data.  */

--Step 3: BigQuery - MERGE for Upsert
--Stage incremental data first, then apply MERGE:

-- Deduplicate staging table
CREATE OR REPLACE TABLE dataset.staging_dedup AS
SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
FROM dataset.staging_table;

-- MERGE for upsert (insert new, update existing)
MERGE dataset.target_table T
USING dataset.staging_dedup S
ON T.id = S.id AND S.rn = 1
WHEN MATCHED THEN
  UPDATE SET 
    T.name = S.name,
    T.updated_at = S.updated_at
WHEN NOT MATCHED THEN
  INSERT (id, name, updated_at)
  VALUES (S.id, S.name, S.updated_at);

--This pattern handles INSERT + UPDATE in one operation.

/* Step 4: BigQuery Table Optimization
Create your target table with partitioning + clustering: */
CREATE TABLE dataset.target_table
PARTITION BY DATE(updated_at)  -- Daily partitioning [web:18]
CLUSTER BY id, name            -- Cluster on join/filter columns [web:12][web:19]
AS SELECT * FROM source_table;

/* | Optimization                  | Purpose                                                             |
| ----------------------------- | ------------------------------------------------------------------- |
| PARTITION BY DATE(updated_at) | Prune partitions during queries, reduce scan cost docs.cloud.google |
| CLUSTER BY id                 | Speed up MERGE on id column datawise                                |
| Minimum 10GB per partition    | Avoid metadata overhead youtube                                     | */

Production Best Practices
Always stage incremental data first → Deduplicate → Then MERGE

Store watermark in metadata table → Query MAX(updated_at) from target after each run

Handle late-arriving data → Sliding window: reload last 3 days every run

Add audit columns → etl_loaded_at, etl_source_batch_id for traceability

Use BigQuery Storage Write API for high-rate streaming inserts

When to  choose dataflow service when compare to other gcp services!!
| Use Case                                        | Choose Dataflow             | Choose Alternative                                     |
| ----------------------------------------------- | --------------------------- | ------------------------------------------------------ |
| Streaming data (real-time)                      | ✅ Dataflow                  | ❌ Dataproc, BigQuery only for batch                    |
| Windowing + watermarks + late data              | ✅ Dataflow                  | ❌ Cloud Functions, Cloud Run                           |
| Large datasets (TB+) with complex ETL           | ✅ Dataflow                  | ❌ Cloud Functions (<9 min limit) stackoverflow+1       |
| Apache Beam pipeline (existing/migrating)       | ✅ Dataflow                  | ❌ All others                                           |
| Greenfield pipeline, no Hadoop/Spark dependency | ✅ Dataflow                  | ❌ Dataproc                                             |
| Serverless, autoscaling, no cluster management  | ✅ Dataflow                  | ❌ Dataproc (cluster control needed) gcpstudyhubyoutube |
| Aggregations across multiple elements           | ✅ Dataflow                  | ❌ Cloud Functions (1:1 only) stackoverflow             |
| Long-running pipeline (hours/days)              | ✅ Dataflow                  | ❌ Cloud Functions (9-min timeout) stackoverflow        |
| Legacy Spark/Hadoop workload                    | ❌ Dataproc                  | ✅ Dataproc (no rewrite needed) gcpstudyhub+1           |
| Simple event-driven micro-batch                 | ❌ Cloud Run/Cloud Functions | ✅ Cloud Run (scales to zero) linkedin                  |
| SQL-only analytics, dashboards                  | ❌ BigQuery                  | ✅ BigQuery (no code needed) eitca+1                    |