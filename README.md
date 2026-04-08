# ECS Zambia Public Repository

This repository contains the code and analysis for the Kitchen Performance Test (KPT) sensor data study in Zambia, focusing on fuel efficiency interventions.

## Project Structure

- `Datenanalyse/` - Data analysis scripts and notebooks
- `ECS_EXACT/` - EXACT sensor data (original data excluded via .gitignore)
- `ECS_FUEL/` - Fuel consumption data (original data excluded via .gitignore)
- `ECS_RAW/` - Raw sensor data (excluded via .gitignore due to sensitivity)
- `ECS_Skripte_python/` - Python scripts for data processing
- `dummy_data/` - Sample data files for testing and demonstration

## Data Note

Due to privacy and sensitivity concerns, the original sensor data files (`ECS_EXACT/`, `ECS_FUEL/`, `ECS_RAW/`) are excluded from this repository via `.gitignore`. For users who want to run the code, dummy data files are provided in the `dummy_data/` directory with the same format as the original data.

## Getting Started

1. Clone this repository
2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Use the dummy data in `dummy_data/` for testing, or replace with your own data following the same directory structure and file naming conventions.

## Requirements

See `requirements.txt` for Python dependencies.

## License

[Specify your license here]