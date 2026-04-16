# AGENTS.md

## Project Overview

ECS Zambia Public — Kitchen Performance Test data pipeline. Contains:
- **Python scripts** (`ECS_Skripte_python/`) — Data cleaning, sorting, and ETL processing
- **FastAPI webapp** (`webapp/`) — CSV upload, database exploration, and SQL query interface
- **Data folders** (`ECS_RAW/`, `ECS_FUEL/`, `ECS_EXACT/`, `Datenanalyse/`) — Sensor CSV data and SQLite database

## Build / Run Commands

### Webapp
```bash
# Install dependencies
pip install -r requirements.txt

# Run development server
cd webapp && python run.py
# Or: python -m uvicorn app:app --reload --port 8000

# Run production
python -m uvicorn webapp.app:app --host 0.0.0.0 --port 8000
```

### Python Scripts
```bash
# Main ETL pipeline (sort → dedupe → database)
cd ECS_Skripte_python && python main.py

# Individual scripts
python ECS_Skripte_python/hh_id_sort.py      # Sort files by household ID
python ECS_Skripte_python/remove_duplicates.py  # Remove duplicate files
python ECS_Skripte_python/database.py        # Build flat database (legacy)
python ECS_Skripte_python/db_4table.py       # Build relational database
```

### Testing
- **No formal test framework exists** — add `pytest` and write tests in `tests/` when needed
- Manual testing: Run scripts and verify SQLite database in `Datenanalyse/ECS_Database.db`

### Linting (if added)
```bash
# Python linting (install ruff first: pip install ruff)
ruff check .
ruff check ECS_Skripte_python/ webapp/

# Format code
ruff format .
```

---

## Code Style Guidelines

### Python — General

| Convention | Rule |
|------------|------|
| **Indentation** | 4 spaces (no tabs) |
| **Line length** | Max 120 characters |
| **File encoding** | UTF-8 |
| **String quotes** | Prefer single quotes `'` — but be consistent within a file |
| **Type hints** | Use for function parameters and return values when not obvious |
| **Docstrings** | Use triple-quoted docstrings for modules and public functions |

### Python — Imports

Order imports in groups with blank lines between:
1. Standard library (`os`, `sqlite3`, `glob`, `datetime`)
2. Third-party packages (`pandas`, `fastapi`, `pydantic`)
3. Local/relative imports (`.`, `..`)

```python
import os
import sqlite3
from datetime import datetime

import pandas as pd
from fastapi import FastAPI

from . import local_module
```

### Python — Naming

| Element | Convention | Example |
|---------|------------|---------|
| Modules | snake_case | `db_4table.py` |
| Functions | snake_case | `process_csv_files()` |
| Classes | PascalCase | `QueryRequest` |
| Constants | UPPER_SNAKE | `MAX_RESULT_ROWS` |
| Variables | snake_case | `fuel_type`, `conn` |

### Python — Error Handling

- Use bare `except Exception` sparingly — catch specific exceptions when possible
- Always close database connections in `finally` blocks
- Return error dicts or raise HTTPException (FastAPI) — don't silently swallow errors in web endpoints
- Print user-friendly error messages with file context

```python
try:
    result = process_csv_files(...)
except Exception as e:
    print(f"Error processing {file}: {e}")
    # Re-raise or handle appropriately
```

### Python — Database (SQLite)

- Always set `timeout` on connection (e.g., 30 seconds)
- Use parameterized queries — never string concatenation for SQL
- Always close connections in `finally` blocks
- Use `row_factory = sqlite3.Row` for dict-like access

```python
conn = sqlite3.connect(DB_PATH, timeout=30)
conn.row_factory = sqlite3.Row
try:
    # queries
finally:
    conn.close()
```

### FastAPI Specific

- Use Pydantic models (`BaseModel`) for request/response validation
- Use `async` for route handlers
- Return `HTTPException` for errors with appropriate status codes
- Validate file uploads before processing

### JavaScript — General

| Convention | Rule |
|------------|------|
| **Indentation** | 4 spaces |
| **Line length** | Max 120 characters |
| **Semicolons** | Required |
| **Strict mode** | Always use `'use strict'` in IIFEs |
| **Quotes** | Single quotes preferred |

```javascript
(() => {
    'use strict';
    // code
})();
```

### JavaScript — Naming

| Element | Convention | Example |
|---------|------------|---------|
| Variables/Functions | camelCase | `uploadFiles()`, `lastQueryRows` |
| DOM refs | Prefix with `$` | `const $statusDot = $('#status-dot')` |
| Constants | UPPER_SNAKE | `const API = '';` |

### JavaScript — Patterns

- Use IIFE pattern with strict mode for module encapsulation
- Use `async/await` for API calls
- Always handle errors in try/catch blocks
- Use template literals for string concatenation
- Escape HTML when rendering user data: use `escHtml()` utility

```javascript
async function uploadFiles(files) {
    try {
        const res = await fetch(`${API}/api/upload`, { method: 'POST', body: formData });
        const data = await res.json();
        renderUploadResults(data.results);
    } catch (err) {
        toast(`Upload error: ${err.message}`, 'error');
    }
}
```

### CSS

- Use kebab-case for class names
- Use CSS custom properties (variables) for theming
- Prefer classes over inline styles
- Use flexbox/grid for layouts — avoid floats

```css
.result-card {
    display: flex;
    gap: var(--spacing-md);
    /* Use CSS variables */
}
```

---

## File Organization

```
ECS_Zambia_Public/
├── AGENTS.md              # This file
├── requirements.txt       # Python dependencies
├── ECS_Skripte_python/    # Data processing scripts
│   ├── main.py            # Entry point
│   ├── database.py        # Legacy flat DB builder
│   ├── db_4table.py       # Relational DB builder
│   ├── hh_id_sort.py      # File sorting
│   └── remove_duplicates.py
├── webapp/                # FastAPI application
│   ├── app.py             # Main FastAPI app
│   ├── run.py             # Entry point
│   └── static/            # Frontend assets
├── Datenanalyse/          # Output database location
│   └── ECS_Database.db    # SQLite database
└── ECS_RAW/               # Raw CSV input data
```

---

## Database Schema

Tables created by ETL:
- `fuel_meta` — FUEL sensor metadata (hhid, sensor_id, fuel_type, start_time, fuel_id)
- `fuel_measurement` — FUEL sensor readings (timestamp, usage, fuel_id)
- `exact_meta` — EXACT sensor metadata (hhid, sensor_id, stove_name, max_temp, exact_id)
- `exact_measurement` — EXACT sensor readings (timestamp, usage, gradient, temperature, exact_id)

---

## Common Tasks

### Add a new ETL step
1. Edit `webapp/app.py` — add processing function, then add route
2. Test with sample CSV files
3. Verify data in SQLite database

### Add a new database table
1. Edit `ECS_Skripte_python/db_4table.py` or `webapp/app.py`
2. Run ETL pipeline to create table
3. Test with SQL queries via webapp

### Modify data processing logic
1. Edit the relevant script in `ECS_Skripte_python/`
2. Run `python ECS_Skripte_python/main.py` to reprocess
3. Verify output in database
