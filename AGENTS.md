# AGENTS.md

## Project Overview

ECS Zambia Public — Kitchen Performance Test data pipeline with:
- **Python scripts** (`ECS_Skripte_python/`) — ETL processing for FUEL/EXACT sensor data
- **FastAPI webapp** (`webapp/`) — CSV upload, database exploration, and SQL query interface
- **Data folders** (`ECS_RAW/`, `ECS_FUEL/`, `ECS_EXACT/`, `Datenanalyse/`) — Sensor CSV data and SQLite database

---

## Build / Run Commands

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Webapp
```bash
cd webapp && python run.py
# Or: python -m uvicorn app:app --reload --port 8000
```

### ETL Pipeline
```bash
cd ECS_Skripte_python && python main.py
```

### Testing
- **No formal test framework** — manual verification against `Datenanalyse/ECS_Database.db`
- Run webapp and test SQL queries via UI
- Verify ETL output by checking database tables and row counts

### Linting & Formatting
```bash
pip install ruff
ruff check .                    # Lint entire project
ruff check ECS_Skripte_python/ webapp/  # Lint specific directories
ruff format .                  # Format code
```

---

## Code Style Guidelines

### Python — General

| Convention | Rule |
|------------|------|
| **Indentation** | 4 spaces (no tabs) |
| **Line length** | Max 120 characters |
| **Encoding** | UTF-8 |
| **Quotes** | Single quotes `'` preferred |
| **Type hints** | Use for function params and return values |
| **Docstrings** | Triple-quoted for modules and public functions |

### Python — Imports (ordered groups, blank lines between)

```python
# 1. Standard library
import os
import sqlite3
from datetime import datetime

# 2. Third-party
import pandas as pd
from fastapi import FastAPI

# 3. Local/relative
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

- Catch specific exceptions — avoid bare `except Exception`
- Close database connections in `finally` blocks
- Return error dicts or raise `HTTPException` in FastAPI endpoints
- Print user-friendly messages with file context

```python
try:
    result = process_csv_files(...)
except ValueError as e:
    print(f"Invalid data in {file}: {e}")
    raise
```

### Python — Database (SQLite)

- Always set `timeout=30` on connection
- Use parameterized queries — never string concatenation
- Use `row_factory = sqlite3.Row` for dict-like access
- Context manager pattern for connection lifecycle

```python
conn = sqlite3.connect(DB_PATH, timeout=30)
conn.row_factory = sqlite3.Row
try:
    # queries
finally:
    conn.close()
```

### FastAPI Conventions

- Use Pydantic `BaseModel` for request/response validation
- Use `async` for route handlers
- Return `HTTPException` with appropriate status codes
- Validate file uploads before processing

### JavaScript — General

| Convention | Rule |
|------------|------|
| **Indentation** | 4 spaces |
| **Line length** | Max 120 characters |
| **Semicolons** | Required |
| **Quotes** | Single quotes preferred |
| **Strict mode** | Use `'use strict'` in IIFEs |

### JavaScript — Patterns

- IIFE pattern with strict mode for encapsulation
- `async/await` for API calls
- Error handling in try/catch blocks
- Template literals for string concatenation
- Prefix DOM refs with `$` (e.g., `$statusDot`)

```javascript
(async () => {
    'use strict';
    try {
        const res = await fetch(`${API}/api/upload`, { method: 'POST', body: formData });
        renderUploadResults(await res.json());
    } catch (err) {
        toast(`Upload error: ${err.message}`, 'error');
    }
})();
```

### CSS

- Kebab-case for class names
- CSS custom properties (variables) for theming
- Prefer classes over inline styles
- Flexbox/grid for layouts — avoid floats

---

## File Organization

```
ECS_Zambia_Public/
├── AGENTS.md
├── requirements.txt
├── ECS_Skripte_python/
│   ├── main.py              # ETL pipeline entry
│   ├── db_4table.py         # Relational DB builder
│   ├── hh_id_sort.py        # File sorting
│   └── remove_duplicates.py
├── webapp/
│   ├── app.py               # FastAPI routes
│   ├── database.py          # DB connection
│   ├── etl.py               # CSV processing
│   ├── run.py              # Entry point
│   └── static/             # Frontend assets
├── Datenanalyse/
│   └── ECS_Database.db      # SQLite database
└── ECS_RAW/                 # Raw CSV input
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
1. Edit `webapp/etl.py` — add processing function
2. Add route in `webapp/app.py`
3. Test with sample CSV files

### Modify data processing
1. Edit script in `ECS_Skripte_python/`
2. Run ETL pipeline
3. Verify output in database
