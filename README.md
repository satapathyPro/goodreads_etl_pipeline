# GoodReads Data Pipeline

<p align="center">
  <img src="https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/goodreads.png" width="600">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.6%2B-blue?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/AWS-S3-orange?logo=amazon-aws&logoColor=white" alt="AWS S3">
  <img src="https://img.shields.io/badge/AWS-Redshift-8C4FFF?logo=amazon-aws&logoColor=white" alt="Redshift">
  <img src="https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apache-airflow&logoColor=white" alt="Airflow">
  <img src="https://img.shields.io/badge/Apache-Spark-E25A1C?logo=apache-spark&logoColor=white" alt="Spark">
  <img src="https://img.shields.io/badge/EMR-AWS-FF9900?logo=amazon-aws&logoColor=white" alt="EMR">
  <img src="https://img.shields.io/badge/License-MIT-green" alt="MIT License">
</p>

<p align="center">
  <strong>Production-grade end-to-end data pipeline ingesting Goodreads API data into AWS S3, transforming at scale with PySpark on EMR, and loading into Amazon Redshift for analytics — orchestrated entirely by Apache Airflow.</strong>
</p>

---

## Architecture Overview

![Pipeline Architecture](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/architecture.png)

The pipeline is structured around five sequential stages:

```
Goodreads API  →  S3 Landing Zone  →  ETL Transform (Spark/EMR)  →  Redshift Warehouse  →  Analytics Layer
```

| Stage | Technology | Role |
|-------|-----------|------|
| Ingestion | Goodreads Python Wrapper | Pulls book, shelf, and review data from the API |
| Landing | AWS S3 (Landing Bucket) | Raw data store — immutable, timestamped drops |
| Working Zone | AWS S3 (Working Bucket) | Staging area before Spark transformation |
| Transform | PySpark on AWS EMR | Cleans, repartitions, and enriches datasets |
| Processed Zone | AWS S3 (Processed Bucket) | Parquet output ready for warehouse load |
| Warehouse | Amazon Redshift | Analytical tables with UPSERT semantics |
| Analytics | Custom Airflow Operator | Runs analytical queries post-load |
| Orchestration | Apache Airflow DAG | Schedules, monitors, and quality-checks every step |

---

## Pipeline Components

### 1. GoodReads Python Wrapper
A custom Python wrapper around the Goodreads API that handles authentication, pagination, and rate limiting. View the wrapper at [satapathyPro/goodreads](https://github.com/satapathyPro/goodreads) and its usage in the [Fetch Data Module](https://github.com/satapathyPro/goodreads/blob/master/example/fetchdata.py).

### 2. ETL Jobs (PySpark on EMR)
Spark jobs executed on a 3-node AWS EMR cluster (`m5.xlarge`, 4 vCore / 16 GiB each). Each job:
- Reads raw JSON from the S3 working zone
- Applies schema enforcement, deduplication, and field normalization
- Repartitions the dataset for optimal Redshift `COPY` performance
- Writes Parquet output to the S3 processed zone

### 3. S3 Module
Handles all inter-bucket data movement within the pipeline:
- Copies new files from the **landing zone** into the **working zone** before each Spark run
- Archives processed files to prevent duplicate processing

### 4. Redshift Warehouse Module
Connects to the Redshift cluster via `psycopg2` and manages the two-phase load:
1. **Stage**: Bulk-loads processed Parquet into staging tables using the Redshift `COPY` command
2. **UPSERT**: Merges staging data into production warehouse tables, handling inserts and updates idempotently

### 5. Analytics Module
A Custom Airflow Operator executes pre-defined analytical queries on the warehouse after each successful ETL run. Aggregations cover reading trends, book ratings distributions, shelf popularity, and author statistics.

### 6. Data Quality Checks
Airflow operators validate row counts and null rates on both warehouse tables and analytics output tables after every pipeline execution. Failed checks trigger configurable email alerts.

### 7. GoodReads Faker (Load Testing)
A synthetic data generator (`goodreadsfaker`) that produces realistic Goodreads-shaped data at arbitrary scale — used to stress-test the pipeline under production-representative loads.

---

## Data Flow

```
                         ┌─────────────────────────────────────────────────────────┐
                         │                  Apache Airflow DAG                      │
                         │            (Scheduled every 10 minutes)                  │
                         └─────────────────────┬───────────────────────────────────┘
                                               │
          ┌────────────────────────────────────▼──────────────────────────────────────┐
          │                                                                            │
  ┌───────▼───────┐    ┌──────────────┐    ┌──────────────┐    ┌────────────────────┐│
  │ Goodreads API │───►│  S3 Landing  │───►│  S3 Working  │───►│  PySpark / EMR     ││
  │  (via Wrapper)│    │     Zone     │    │     Zone     │    │  (Transform + DQ)  ││
  └───────────────┘    └──────────────┘    └──────────────┘    └────────┬───────────┘│
                                                                         │            │
                        ┌────────────────────────────────────────────────▼──────────┐ │
                        │                 S3 Processed Zone (Parquet)               │ │
                        └────────────────────────────────┬──────────────────────────┘ │
                                                         │                            │
                        ┌────────────────────────────────▼──────────────────────────┐ │
                        │              Redshift Staging Tables (COPY)               │ │
                        └────────────────────────────────┬──────────────────────────┘ │
                                                         │                            │
                        ┌────────────────────────────────▼──────────────────────────┐ │
                        │          Redshift Warehouse Tables (UPSERT)               │ │
                        └────────────────────────────────┬──────────────────────────┘ │
                                                         │                            │
                        ┌────────────────────────────────▼──────────────────────────┐ │
                        │              Analytics Layer + Data Quality               │ │
                        └───────────────────────────────────────────────────────────┘ │
          └────────────────────────────────────────────────────────────────────────────┘
```

---

## Quickstart / Environment Setup

### Prerequisites
- AWS account with IAM permissions for S3, EMR, Redshift, and EC2
- Python 3.6+
- Apache Airflow deployed (see [Airflow CloudFormation setup](https://github.com/satapathyPro/Data_Engineering_Projects/blob/master/Airflow_Livy_Setup_CloudFormation.md))

### 1. Set Up Airflow

Deploy Airflow on EC2 using the provided CloudFormation template. Then install the SSH tunnel provider:

```bash
pip install apache-airflow[sshtunnel]
```

Copy the `dags/` and `plugins/` folders into the Airflow home directory on EC2, then configure connections per the [Airflow Connections guide](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/Airflow_Connections.md).

> **Note:** This setup provisions an EC2 instance and a Postgres RDS instance. Review AWS charges before launching the CloudFormation stack.

### 2. Set Up EMR

Spin up a 3-node EMR cluster (`m5.xlarge`) using the [AWS EMR Guide](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html) or the IaC script.

Install ETL dependencies on the cluster:

```bash
# psycopg2 for Redshift connectivity
sudo yum install postgresql-libs postgresql-devel
sudo pip-3.6 install psycopg2

# boto3 for S3 operations
pip-3.6 install boto3 --user
```

Set PySpark to use Python 3:

```bash
export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_PYTHON=python3
```

Copy ETL scripts to the EMR master node.

### 3. Set Up Redshift

Provision a 2-node Redshift cluster (`dc2.large`) via the AWS console or use the [Redshift IaC script](https://github.com/satapathyPro/Data_Engineering_Projects/blob/master/Redshift_Cluster_IaC.py) for automated cluster creation.

### 4. Run the Pipeline

Start the Airflow webserver and scheduler, then open the Airflow UI:

```
http://<ec2-instance-ip>:<configured-port>
```

Enable the GoodReads pipeline DAG and trigger a run.

---

## Airflow DAG Views

**Pipeline DAG:**
![Pipeline DAG](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/goodreads_dag.PNG)

**DAG Graph View:**
![DAG View](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/DAG.PNG)

**DAG Tree View:**
![DAG Tree](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/DAG_tree_view.PNG)

**DAG Gantt View:**
![DAG Gantt View](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/DAG_Gantt.PNG)

---

## Performance & Scale

The pipeline was stress-tested using the `goodreadsfaker` synthetic data generator. Results:

| Metric | Value |
|--------|-------|
| Test dataset size | **11.4 GB** per 10-minute cycle |
| Throughput | **~68 GB / hour** |
| Projected daily volume | **~1.6 TB / day** |
| EMR cluster | 3x `m5.xlarge` (4 vCore, 16 GiB, EBS 64 GiB each) |
| Redshift cluster | 2x `dc2.large` |

**Source Dataset Count:**
![Source Dataset Count](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/DatasetCount.PNG)

**DAG Run Results:**
![GoodReads DAG Run](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/DAG_tree_view.PNG)

**Data Loaded to Warehouse:**
![GoodReads Warehouse Count](https://github.com/satapathyPro/goodreads_etl_pipeline/blob/master/docs/images/WarehouseCount.PNG)

---

## Scaling Scenarios

| Scenario | Approach |
|----------|----------|
| **Data increases 100x** | Scale up EMR cluster node count; Redshift handles read-heavy analytical workloads natively |
| **Schedule changed to daily 7 AM** | Update the Airflow DAG cron expression; data quality operators and email alerting remain in place |
| **100+ concurrent users on analytics** | Redshift supports up to 50 parallel queries per cluster; launch additional clusters for higher concurrency |

---

## Maintainer

**Subham Satapathy** — Software Engineer with 6+ years building cloud-scale distributed systems and high-performance data pipelines. Specializes in Spark optimization, production observability, and end-to-end automated workflow architecture.

- GitHub: [satapathyPro](https://github.com/satapathyPro)
- LinkedIn: [subhamumd](https://www.linkedin.com/in/subhamumd/)
- Email: satapathypro@gmail.com
