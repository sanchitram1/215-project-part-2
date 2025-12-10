# Agents Guide for Voyla OLAP DB Project

## Project Overview

This project implements an OLAP (Online Analytical Processing) database system for voyla.world. The system extracts data from an OLTP (Online Transaction Processing) source and transforms it into a star schema optimized for analytical queries. The final deliverable includes an interactive dashboard showing user clusters based on content, place, and property appearance.

**Key Components:**
1. **OLAP Schema:** Star schema with a central fact table and multiple dimension tables
2. **ETL Pipeline:** Extract data from OLTP source and load into OLAP database using psycopg2
3. **Dashboard:** Visualization of user clusters across content/place/property dimensions

## Technology Stack

- **Python Version:** 3.13+
- **Package Manager:** `uv` (use `uv add`, `uv sync` instead of pip)
- **Linting & Formatting:** `ruff` (use `ruff check` and `ruff format`)
- **Testing:** `pytest` with standard project layout
- **Database Driver:** `psycopg2` for PostgreSQL connections
- **Data Processing:** pandas, numpy

## Project Structure

```
215-project-part-2/
├── pipeline/              # ETL pipeline code
│   ├── extract.py        # Data extraction from OLTP source
│   ├── transform.py      # Data transformation logic
│   ├── load.py           # Data loading into OLAP database
│   └── __init__.py
├── sql/
│   ├── schema.sql        # OLAP schema definition (dimension & fact tables)
│   └── OLAP schema.jpg   # Visual schema diagram
├── tests/                # Test files (pytest)
│   ├── test_pipeline.py
│   ├── test_transform.py
│   └── __init__.py
├── main.py              # Entry point / orchestrator
├── pyproject.toml       # Project dependencies and metadata
├── README.md            # User-facing documentation
└── AGENTS.md            # This file
```

## Schema Overview

**Dimension Tables:**
- `users` - User dimension with user_id
- `content` - Content dimension with content_id, likes, upload_time, comments, social_media_data
- `places` - Place dimension with place_id (from Google Maps/GenAI)
- `property` - Property dimension with property_id

**Fact Table:**
- Central fact table containing: user_id, content_id, place_id, property_id
- Links users, content, places, and properties together for analytical queries

## Coding Standards

### Python Style & Formatting

1. **Code Formatting:** Run `ruff format` before committing
   ```bash
   ruff format .
   ```

2. **Linting:** Run `ruff check` to identify issues
   ```bash
   ruff check .
   ruff check --fix .  # Auto-fix common issues
   ```

3. **Style Guidelines:**
   - Follow PEP 8 conventions (ruff enforces this)
   - Use descriptive variable and function names
   - Add docstrings to all functions and classes
   - Maximum line length: 88 characters (ruff default)
   - Use type hints for function arguments and return values

4. **Import Organization:**
   - Standard library imports first
   - Third-party imports second (pandas, psycopg2, numpy)
   - Local imports last
   - Separate groups with blank lines

### Function & Class Design

1. **Database Functions:**
   - All database connections should use context managers
   - Always close cursors after use
   - Include error handling for connection failures
   - Example:
   ```python
   def get_data_from_source(connection_params: dict) -> pd.DataFrame:
       """Fetch data from OLTP source."""
       try:
           with psycopg2.connect(**connection_params) as conn:
               with conn.cursor() as cur:
                   cur.execute("SELECT * FROM source_table")
                   return pd.DataFrame(cur.fetchall())
       except psycopg2.Error as e:
           raise ValueError(f"Database error: {e}")
   ```

2. **ETL Functions:**
   - Separate extract, transform, and load logic into distinct modules
   - Each function should have a single responsibility
   - Pass DataFrames between functions (not raw database connections)
   - Use pandas operations for data transformation

3. **Error Handling:**
   - Catch specific exceptions (not generic `Exception`)
   - Log errors with context before raising
   - Provide meaningful error messages

### Testing

1. **Test Structure:**
   - Place tests in `tests/` directory with `test_` prefix
   - Use pytest fixtures for setup/teardown
   - Test file names should mirror module names (e.g., `test_transform.py` for `pipeline/transform.py`)

2. **Test Coverage:**
   - Test extract functions with mocked database connections
   - Test transform functions with sample data
   - Test load functions with test database

3. **Running Tests:**
   ```bash
   pytest              # Run all tests
   pytest -v           # Verbose output
   pytest tests/test_transform.py  # Run specific test file
   ```

### Documentation

1. **Module Docstrings:**
   ```python
   """
   Extract module for ETL pipeline.
   
   Handles connection to OLTP source and retrieves raw data
   for transformation.
   """
   ```

2. **Function Docstrings:**
   ```python
   def extract_users(connection_params: dict) -> pd.DataFrame:
       """
       Extract user data from OLTP source.
       
       Args:
           connection_params: Dictionary with host, user, password, dbname
           
       Returns:
           DataFrame with columns: user_id, [user columns]
           
       Raises:
           ValueError: If database connection fails
       """
   ```

3. **Comments:**
   - Use comments to explain *why*, not *what*
   - Keep comments updated with code changes

## Workflow Commands

### Package Management
```bash
uv add <package>          # Add new dependency
uv sync                   # Install dependencies from lock file
uv lock                   # Update lock file
```

### Code Quality
```bash
ruff format .             # Format all Python files
ruff check . --fix        # Lint and auto-fix
```

### Testing
```bash
pytest                    # Run all tests
pytest -v                 # Verbose mode
pytest --cov             # Generate coverage report (if pytest-cov installed)
```

### Running the Pipeline
```bash
python main.py            # Run full ETL pipeline
```

## Git Workflow

1. Create a branch for each feature/fix: `git checkout -b feature/descriptive-name`
2. Commit with clear messages: `git commit -m "Add ETL extraction logic"`
3. Before pushing, ensure:
   - Code is formatted: `ruff format .`
   - Code passes linting: `ruff check .`
   - All tests pass: `pytest`
4. Push and create pull request for team review

## Database Configuration

Connection parameters should be stored in environment variables or a configuration file:
```python
import os

DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD"),
    "dbname": os.getenv("DB_NAME", "olap_db"),
}
```

## Development Tips

- Use pandas `head()`, `info()`, and `describe()` to inspect DataFrames during development
- Test transformations on small subsets before running on full datasets
- Log progress during long-running pipeline operations
- Keep source data separate from OLAP data (never modify source directly)

## Questions or Issues?

Refer to project documentation in README.md or check the schema diagram in sql/OLAP schema.jpg
