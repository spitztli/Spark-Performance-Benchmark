"""
Configuration module for Spark Performance Benchmark.

This module defines constants for paths, filenames, and ensures that
necessary directories are created automatically.

Author: Senior Data Engineer
Date: 2025-12-08
"""

import os
import sys
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


def configure_hadoop_home() -> None:
    """
    Configure HADOOP_HOME environment variable for Windows.
    
    This function is essential for running Spark on Windows. Spark uses Hadoop
    libraries internally, which require native Windows binaries (winutils.exe, hadoop.dll).
    
    The function:
    1. Checks if HADOOP_HOME is already set
    2. If not, sets it to C:\\hadoop (default location)
    3. Verifies that required binaries exist
    4. Sets the environment variable for the current Python process
    
    This MUST be called BEFORE creating a SparkSession.
    
    Returns:
        None
        
    Raises:
        FileNotFoundError: If Hadoop binaries are not found in the expected location
        
    Example:
        >>> configure_hadoop_home()
        ✓ HADOOP_HOME configured: C:\\hadoop
    """
    # Check if HADOOP_HOME is already set
    hadoop_home = os.environ.get('HADOOP_HOME')
    
    if hadoop_home:
        print(f"✓ HADOOP_HOME already set: {hadoop_home}")
    else:
        # Set default Hadoop home for Windows
        if sys.platform == 'win32':
            default_hadoop_home = r'C:\hadoop'
            
            # Check if the directory exists
            hadoop_path = Path(default_hadoop_home)
            if not hadoop_path.exists():
                print(f"⚠️  WARNING: Hadoop directory not found at {default_hadoop_home}")
                print("   Spark may encounter errors when writing files.")
                print("\n   To fix this:")
                print("   1. Download Hadoop winutils from: https://github.com/cdarlint/winutils")
                print("   2. Extract to C:\\hadoop")
                print("   3. Ensure winutils.exe and hadoop.dll are in C:\\hadoop\\bin\\")
                return
            
            # Check for required binaries
            bin_path = hadoop_path / 'bin'
            winutils_path = bin_path / 'winutils.exe'
            hadoop_dll_path = bin_path / 'hadoop.dll'
            
            if not winutils_path.exists():
                raise FileNotFoundError(
                    f"winutils.exe not found at {winutils_path}\n"
                    f"Please download Hadoop binaries and place them in {bin_path}"
                )
            
            if not hadoop_dll_path.exists():
                raise FileNotFoundError(
                    f"hadoop.dll not found at {hadoop_dll_path}\n"
                    f"Please download Hadoop binaries and place them in {bin_path}"
                )
            
            # Set the environment variable
            os.environ['HADOOP_HOME'] = default_hadoop_home
            print(f"✓ HADOOP_HOME configured: {default_hadoop_home}")
            print(f"✓ Found winutils.exe: {winutils_path}")
            print(f"✓ Found hadoop.dll: {hadoop_dll_path}")
        else:
            # Non-Windows systems don't need this
            print("ℹ️  HADOOP_HOME configuration skipped (not Windows)")


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
    print("Testing Hadoop Configuration:")
    print("=" * 60)
    configure_hadoop_home()
    print("\n" + "=" * 60)
