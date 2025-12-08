"""
Configuration module for Spark Performance Benchmark.

This module defines constants for paths, filenames, and ensures that
necessary directories are created automatically.

Author: Senior Data Engineer
Date: 2025-12-08
"""

import os
from pathlib import Path
from typing import Final

# Base directories
PROJECT_ROOT: Final[Path] = Path(__file__).parent.parent
DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
RESULTS_DIR: Final[Path] = PROJECT_ROOT / "results"
NOTEBOOKS_DIR: Final[Path] = PROJECT_ROOT / "notebooks"
SRC_DIR: Final[Path] = PROJECT_ROOT / "src"

# Data subdirectories
RAW_DATA_DIR: Final[Path] = DATA_DIR / "raw"
PROCESSED_DATA_DIR: Final[Path] = DATA_DIR / "processed"
DICTIONARY_DIR: Final[Path] = DATA_DIR / "dictionary"

# Results subdirectories
PLOTS_DIR: Final[Path] = RESULTS_DIR / "plots"

# File paths
BENCHMARK_LOG_FILE: Final[Path] = RESULTS_DIR / "benchmark_logs.csv"

# Data format directories
CSV_DATA_DIR: Final[Path] = PROCESSED_DATA_DIR / "csv"
PARQUET_DATA_DIR: Final[Path] = PROCESSED_DATA_DIR / "parquet"
DELTA_DATA_DIR: Final[Path] = PROCESSED_DATA_DIR / "delta"

# Dataset names
FACT_SALES_TABLE: Final[str] = "fact_sales"
DIM_CUSTOMERS_TABLE: Final[str] = "dim_customers"

# Spark configuration defaults
SPARK_APP_NAME: Final[str] = "SparkPerformanceBenchmark"
SPARK_MASTER: Final[str] = "local[*]"  # Use all available cores


def ensure_directories_exist() -> None:
    """
    Create all necessary directories if they don't exist.
    
    This function should be called at the start of any script to ensure
    the project structure is properly initialized.
    
    Returns:
        None
        
    Raises:
        OSError: If directory creation fails due to permissions or other OS errors.
    """
    directories = [
        DATA_DIR,
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        DICTIONARY_DIR,
        RESULTS_DIR,
        PLOTS_DIR,
        CSV_DATA_DIR,
        PARQUET_DATA_DIR,
        DELTA_DATA_DIR,
    ]
    
    for directory in directories:
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise OSError(f"Failed to create directory {directory}: {e}") from e
    
    print(f"✓ All directories verified/created successfully")


def get_data_path(format_type: str, table_name: str) -> Path:
    """
    Get the path for a specific data format and table name.
    
    Args:
        format_type: The data format ('csv', 'parquet', or 'delta')
        table_name: The name of the table
        
    Returns:
        Path object pointing to the data location
        
    Raises:
        ValueError: If format_type is not supported
        
    Example:
        >>> get_data_path('parquet', 'fact_sales')
        WindowsPath('c:/Users/.../data/processed/parquet/fact_sales')
    """
    format_type = format_type.lower()
    
    format_dirs = {
        'csv': CSV_DATA_DIR,
        'parquet': PARQUET_DATA_DIR,
        'delta': DELTA_DATA_DIR,
    }
    
    if format_type not in format_dirs:
        raise ValueError(
            f"Invalid format_type '{format_type}'. "
            f"Must be one of: {', '.join(format_dirs.keys())}"
        )
    
    return format_dirs[format_type] / table_name


# Initialize directories when module is imported
ensure_directories_exist()


if __name__ == "__main__":
    # Display configuration when run as script
    print("=" * 60)
    print("Spark Performance Benchmark - Configuration")
    print("=" * 60)
    print(f"\nProject Root: {PROJECT_ROOT}")
    print(f"Data Directory: {DATA_DIR}")
    print(f"Results Directory: {RESULTS_DIR}")
    print(f"\nBenchmark Log File: {BENCHMARK_LOG_FILE}")
    print(f"\nFormat Directories:")
    print(f"  - CSV: {CSV_DATA_DIR}")
    print(f"  - Parquet: {PARQUET_DATA_DIR}")
    print(f"  - Delta: {DELTA_DATA_DIR}")
    print("\n" + "=" * 60)
