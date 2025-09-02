# Data Pipeline with Airflow, MinIO, and PostgreSQL

This project sets up a complete data engineering pipeline using Apache Airflow, MinIO (S3-compatible storage), and PostgreSQL, all running in Docker containers.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Apache Airflow │    │      MinIO      │    │   PostgreSQL    │
│                 │    │   (S3-compatible│    │                 │
│ • Scheduler     │◄──►│    storage)     │    │ • Airflow DB    │
│ • Web UI        │    │                 │    │ • Data Warehouse│
│ • DAGs          │    │ Buckets:        │    │   (dwh)         │
│                 │    │ • bronze        │    │                 │
│                 │    │ • silver        │    │ Schemas:        │
│                 │    │ • gold          │    │ • bronze        │
└─────────────────┘    └─────────────────┘    │ • silver        │
                                              │ • gold          │
                                              └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Git for cloning the repository
- At least 4GB of available RAM

### 1. Clone and Navigate

```bash
git clone <repository-url>
cd data-pipeline-sofascore
```

### 2. Start the Pipeline

```bash
cd docker
docker-compose up -d
```

### 3. Access Services

- **Airflow Web UI**: http://localhost:8080
  - Username: `airflow`
  - Password: `airflow`
- **MinIO Console**: http://localhost:9001
  - Username: `minio`
  - Password: `minio123`
- **PostgreSQL**: `localhost:5432`
  - Database: `dwh`
  - Username: `airflow`
  - Password: `airflow`

## 📦 Components

### Apache Airflow (3.0.6)

**Services:**
- `airflow-webserver` (Port 8080): Web UI and API server
- `airflow-scheduler`: Task scheduling and execution
- `airflow-init`: Database initialization and setup

**Key Features:**
- Latest provider packages for AWS/S3 integration
- Custom Docker image with MinIO support
- Automatic connection and bucket creation
- Fernet encryption for secure password storage

**Environment Configuration:**
- Executor: `LocalExecutor`
- Database: PostgreSQL
- Authentication: Airflow FAB Auth Manager
- Examples: Disabled
- DAGs: Paused at creation

### MinIO Storage

**Configuration:**
- **API Port**: 9000
- **Console Port**: 9001
- **Root User**: `minio`
- **Root Password**: `minio123`

**Auto-created Buckets:**
- `bronze`: Raw data storage
- `silver`: Cleaned/transformed data
- `gold`: Analytics-ready data

### PostgreSQL Database

**Configuration:**
- **Port**: 5432
- **Database**: `dwh` (Data Warehouse)
- **User/Password**: `airflow/airflow`

**Auto-created Schemas:**
- `bronze`: Raw data tables
- `silver`: Processed data tables  
- `gold`: Analytics tables

## 🔧 Technical Implementation

### Docker Compose Structure

```yaml
services:
  postgres:     # Database for Airflow metadata and data warehouse
  minio:        # S3-compatible object storage
  airflow-init: # One-time setup (DB migration, user creation, connections)
  airflow-webserver: # Web UI and API
  airflow-scheduler: # Task execution engine
```

### Custom Airflow Image

Built with additional packages:
- `apache-airflow-providers-amazon==9.12.0`
- `boto3==1.40.21`
- `minio==7.2.16`

### Automatic Setup Scripts

**Connection Creation** (`create_minio_conn.sh`):
- Creates MinIO S3 connection in Airflow
- Connection ID: `minio_s3`
- Region: `eu-central-1` (Central Europe)
- Idempotent: Only creates if doesn't exist

**Bucket Creation** (`create_minio_buckets.sh`):
- Creates bronze/silver/gold buckets in MinIO
- Uses Airflow S3Hook for integration testing
- Idempotent: Skips existing buckets

### Security Features

- **Fernet Key**: Fixed encryption key for password security
- **Health Checks**: PostgreSQL and scheduler monitoring
- **Restart Policies**: Automatic container recovery

## 🗂️ Project Structure

```
data-pipeline-sofascore/
├── docker/
│   ├── docker-compose.yml     # Main orchestration file
│   ├── Dockerfile            # Custom Airflow image
│   ├── requirements.txt      # Python dependencies
│   ├── dags/                # Airflow DAGs (mounted)
│   ├── logs/                # Airflow logs (mounted)
│   └── plugins/             # Airflow plugins (mounted)
├── airflow/
│   ├── scripts/
│   │   ├── create_minio_conn.sh      # MinIO connection setup
│   │   └── create_minio_buckets.sh   # Bucket creation
│   └── dags/
│       └── init_minio_buckets.py     # Sample DAG
├── postgres/
│   └── init/
│       └── create_schemas.sql        # Database schema setup
└── README.md
```

## 🔌 Connections

### MinIO S3 Connection

**Connection Details:**
- **Conn ID**: `minio_s3`
- **Conn Type**: `AWS`
- **Login**: `minio`
- **Password**: `minio123`
- **Extra**: 
  ```json
  {
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123", 
    "endpoint_url": "http://minio:9000",
    "region_name": "eu-central-1"
  }
  ```

### Usage in DAGs

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Use the pre-configured connection
s3_hook = S3Hook(aws_conn_id='minio_s3')

# List buckets
buckets = s3_hook.list_buckets()

# Upload file
s3_hook.load_file(
    filename='local_file.csv',
    key='data/file.csv',
    bucket_name='bronze'
)
```

## 🛠️ Development

### Adding New DAGs

1. Place Python files in `docker/dags/`
2. DAGs will be automatically discovered
3. Use the `minio_s3` connection for S3 operations

### Updating Dependencies

1. Modify `docker/requirements.txt`
2. Rebuild image: `docker-compose build --no-cache`
3. Restart services: `docker-compose up -d`

### Accessing Logs

```bash
# Airflow logs
docker logs airflow_scheduler
docker logs airflow_webserver

# Database logs  
docker logs postgres

# MinIO logs
docker logs minio
```

## 🔄 Data Pipeline Workflow

### Typical Flow

1. **Bronze Layer**: Raw data ingestion to MinIO `bronze` bucket
2. **Silver Layer**: Data cleaning and transformation
3. **Gold Layer**: Analytics-ready aggregated data
4. **PostgreSQL**: Structured data storage with schema separation

### Sample DAG Structure

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG('data_pipeline', schedule_interval='@daily')

# Extract: Download data to bronze
extract_task = S3UploadOperator(
    task_id='extract_data',
    bucket_name='bronze',
    # ... configuration
)

# Transform: Process bronze to silver
transform_task = S3UploadOperator(
    task_id='transform_data', 
    bucket_name='silver',
    # ... configuration
)

# Load: Load to PostgreSQL gold schema
load_task = PostgresOperator(
    task_id='load_to_warehouse',
    postgres_conn_id='postgres_default',
    sql='INSERT INTO gold.analytics_table ...',
)

extract_task >> transform_task >> load_task
```

## 🚨 Troubleshooting

### Common Issues

**1. Container startup failures**
```bash
# Check container status
docker-compose ps

# View specific container logs
docker logs <container_name>
```

**2. Connection issues**
```bash
# Test MinIO connection
docker exec airflow_webserver airflow connections get minio_s3

# Test database connection
docker exec postgres psql -U airflow -d dwh -c "\\l"
```

**3. Permission issues**
```bash
# Fix script permissions
chmod +x airflow/scripts/*.sh
```

**4. Volume issues**
```bash
# Reset volumes (WARNING: deletes data)
docker-compose down -v
docker volume prune
```

### Logs Location

- **Airflow**: `docker/logs/`
- **Container logs**: `docker logs <container_name>`
- **PostgreSQL**: Inside container at `/var/log/postgresql/`

## 🔐 Security Considerations

- Change default passwords in production
- Use environment variables for sensitive data
- Enable HTTPS for web interfaces
- Implement proper backup strategies
- Regular security updates for base images

## 📈 Monitoring

### Health Checks

- PostgreSQL: Built-in health check
- Airflow Scheduler: Job monitoring
- MinIO: REST API status

### Metrics Access

- **Airflow**: Web UI dashboard at http://localhost:8080
- **MinIO**: Console at http://localhost:9001
- **PostgreSQL**: Connect via any PostgreSQL client

## 🚀 Production Deployment

### Recommendations

1. **Use external databases** for PostgreSQL
2. **Implement proper secrets management**
3. **Set up monitoring and alerting**
4. **Configure backup strategies**
5. **Use production-grade image tags**
6. **Implement CI/CD pipelines**

### Environment Variables

Create `.env` file for production:
```bash
POSTGRES_PASSWORD=secure_password
MINIO_ROOT_PASSWORD=secure_password
AIRFLOW_FERNET_KEY=your_fernet_key
```

---

## 📝 License

This project is licensed under the MIT License.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch  
5. Create a Pull Request
