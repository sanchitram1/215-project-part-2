# Voyla OLAP Database Project

An OLAP (Online Analytical Processing) database implementation for voyla.world
that enables analytical queries across user behavior, content, places, and
properties.

## Project Description

This project implements a complete data warehouse solution that:

1. **Extracts** data from an OLTP (Online Transaction Processing) source
   database
2. **Transforms** the data into a star schema optimized for analytics
3. **Loads** the processed data into an OLAP PostgreSQL database
4. **Visualizes** insights via a dashboard showing user clusters based on
   content, place, and property dimensions

### Architecture

The OLAP schema uses a star schema design with:

- **Fact Table:** Central table linking users, content, places, and properties
- **Dimension Tables:**
  - `users` - User information
  - `content` - Post/content data (likes, upload time, comments, social media
    metrics)
  - `places` - Geographic locations (generated via GenAI and Google Maps API)
  - `property` - Property/listing information

For a visual overview, see `sql/OLAP schema.jpg`

## Getting Started

### Prerequisites

- Python 3.13 or higher
- PostgreSQL (for OLAP database)
- Git
- [uv for python dependency management](https://docs.astral.sh/uv/#installation)

### Installation

1. **Clone the repository:**
```bash git clone https://github.com/sanchitram1/215-project-part-2.git cd
215-project-part-2 ```

2. **Set up Python environment with uv:**
```bash # Install dependencies uv sync ```

3. **Configure database connection:**
Create a `.env` file in the project root (or set environment variables): ```
DB_HOST=localhost DB_USER=postgres DB_PASSWORD=your_password DB_NAME=olap_db ```

4. **Initialize the OLAP database schema:**
```bash psql -U postgres -d olap_db -f sql/schema.sql ```

## Usage

### Running the ETL Pipeline

Execute the complete ETL pipeline:
```bash
python main.py
```

This will:
- Extract data from the OLTP source
- Transform and validate the data
- Load into the OLAP database
- Display progress and completion status

### Development Workflow

**Code Formatting:**
```bash
ruff format .
```

**Code Quality Checks:**
```bash
ruff check .
ruff check . --fix  # Auto-fix issues
```

**Running Tests:**
```bash
pytest              # Run all tests
pytest -v           # Verbose output
pytest tests/test_transform.py  # Run specific test
```

**Adding Dependencies:**
```bash
uv add package_name
```

## Project Structure

```
├── pipeline/               # ETL pipeline modules
│   ├── extract.py         # Data extraction from OLTP
│   ├── transform.py       # Data transformation logic
│   ├── load.py            # Data loading to OLAP
│   └── __init__.py
├── sql/
│   ├── schema.sql         # OLAP database schema
│   └── OLAP schema.jpg    # Visual schema diagram
├── tests/                 # Unit and integration tests
│   ├── test_pipeline.py
│   ├── test_transform.py
│   └── __init__.py
├── main.py                # Entry point - orchestrates ETL
├── pyproject.toml         # Python project configuration
├── README.md              # This file
└── AGENTS.md              # Developer guide and coding standards
```

## Technology Stack

- **Language:** Python 3.13+
- **Package Manager:** `uv` (modern, fast Python package manager)
- **Linting & Formatting:** `ruff` (fast Python linter and formatter)
- **Testing:** `pytest` (Python testing framework)
- **Database Driver:** `psycopg2` (PostgreSQL adapter)
- **Data Processing:** pandas, numpy

## Core Modules

### pipeline/extract.py
Handles connection to OLTP source database and retrieves raw data. Returns
pandas DataFrames with unmodified source data.

### pipeline/transform.py
Applies business logic and data transformation:
- Data cleaning and validation
- Deduplication
- Aggregation for dimension tables
- Schema alignment with OLAP model

### pipeline/load.py
Loads transformed data into OLAP database:
- Inserts into dimension tables (users, content, places, property)
- Inserts into fact table
- Handles conflicts and duplicate prevention
- Logs success/failure status

### main.py
Orchestrates the complete pipeline:
- Chains extract → transform → load
- Handles configuration and environment setup
- Reports overall status and metrics

## Testing

Tests are organized by module and use pytest:

```bash
# Run all tests with coverage
pytest --cov=pipeline tests/

# Run specific test file
pytest tests/test_transform.py -v

# Run specific test function
pytest tests/test_transform.py::test_transform_users -v
```

## Coding Standards

See `AGENTS.md` for detailed coding standards including:
- Python style guidelines (PEP 8 with ruff enforcement)
- Function and class design patterns
- Database interaction best practices
- Testing requirements
- Documentation expectations

## Common Issues & Solutions

**Database Connection Error:**
- Verify PostgreSQL is running
- Check `.env` file has correct credentials
- Ensure OLAP database is created: `psql -U postgres -c "CREATE DATABASE
  olap_db"`

**Import Errors:**
- Run `uv sync` to ensure dependencies are installed
- Check Python version: `python --version` (should be 3.13+)

**Test Failures:**
- Ensure test database is accessible
- Run `ruff format .` and `ruff check .` to fix style issues
- Check test logs for detailed error messages

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make changes following coding standards in `AGENTS.md`
3. Format code: `ruff format .`
4. Run tests: `pytest`
5. Commit with clear messages
6. Push and open a pull request

## Dashboard

The dashboard provides visualization of user clusters based on:
- **Content:** Posts by user engagement metrics
- **Place:** Geographic distribution of interactions
- **Property:** Association with specific properties

Run the dashboard (implementation details in dashboard module):
```bash
python -m dashboard  # Or specific dashboard command when available
```

## License

Project for UC Berkeley IND ENG 215

## Support

For questions about coding standards or development workflow, refer to
`AGENTS.md`. For database schema details, see `sql/OLAP schema.jpg`.

## Tasks

### db-create

```bash
mkdir -p data/db_files
initdb -D data/db_files
```

### db-start

```bash
pg_ctl -D data/db_files -l data/logfile start
```

### db-stop

```bash
pg_ctl -D data/db_files stop
```

### db-kill

Requires: db-stop

```bash
rm -rf data/db_files
```

### db-reset

Requires: db-kill, db-create, db-start

### migrate

```bash
psql $OLAP_DATABASE_URL -f sql/OLAP_schema.sql
```