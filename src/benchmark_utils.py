"""
Benchmark utilities for Spark Performance Testing.

This module provides timing and logging utilities for benchmarking Spark operations,
including a context manager for automatic time measurement and result logging.

Author: Senior Data Engineer
Date: 2025-12-08
"""

import time
import csv
from datetime import datetime
from pathlib import Path
from typing import Optional, Any
from contextlib import contextmanager

from pyspark.sql import SparkSession

from config import BENCHMARK_LOG_FILE


class BenchmarkTimer:
    """
    Context manager for timing Spark operations and logging results.
    
    This class measures execution time, optionally logs file sizes,
    and automatically records results to a CSV log file. It also provides
    functionality to clear Spark caches before benchmarking.
    
    Attributes:
        test_name: Name of the benchmark test
        description: Optional description of the test
        spark: Optional SparkSession for cache clearing
        clear_cache: Whether to clear Spark cache before test
        log_file: Path to the CSV log file
        start_time: Start timestamp of the benchmark
        end_time: End timestamp of the benchmark
        duration: Duration of the benchmark in seconds
        
    Example:
        >>> with BenchmarkTimer("Read Parquet", spark=spark) as timer:
        ...     df = spark.read.parquet("data.parquet")
        ...     df.count()
        >>> print(f"Duration: {timer.duration:.2f}s")
    """
    
    def __init__(
        self,
        test_name: str,
        description: str = "",
        spark: Optional[SparkSession] = None,
        clear_cache: bool = True,
        data_size_mb: Optional[float] = None,
        log_file: Optional[Path] = None
    ):
        """
        Initialize the BenchmarkTimer.
        
        Args:
            test_name: Name identifying this benchmark test
            description: Optional detailed description of the test
            spark: SparkSession instance for cache management
            clear_cache: If True, clears Spark cache before timing
            data_size_mb: Optional size of data being processed (in MB)
            log_file: Optional custom log file path (defaults to BENCHMARK_LOG_FILE)
        """
        self.test_name = test_name
        self.description = description
        self.spark = spark
        self.clear_cache = clear_cache
        self.data_size_mb = data_size_mb
        self.log_file = log_file or BENCHMARK_LOG_FILE
        
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration: Optional[float] = None
        
    def __enter__(self) -> 'BenchmarkTimer':
        """
        Enter the context manager and start timing.
        
        Returns:
            Self reference for access to timing results
        """
        # Clear cache if requested and Spark session is available
        if self.clear_cache and self.spark is not None:
            try:
                self.spark.catalog.clearCache()
                print(f"✓ Cache cleared for: {self.test_name}")
            except Exception as e:
                print(f"⚠ Warning: Could not clear cache: {e}")
        
        # Start timing
        print(f"\n{'='*60}")
        print(f"Starting benchmark: {self.test_name}")
        if self.description:
            print(f"Description: {self.description}")
        print(f"{'='*60}")
        
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        """
        Exit the context manager, stop timing, and log results.
        
        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
            
        Returns:
            False to propagate any exceptions that occurred
        """
        self.end_time = time.time()
        self.duration = self.end_time - self.start_time
        
        # Display results
        print(f"\n{'='*60}")
        print(f"✓ Completed: {self.test_name}")
        print(f"Duration: {self.duration:.3f} seconds ({self.duration/60:.2f} minutes)")
        if self.data_size_mb:
            throughput = self.data_size_mb / self.duration
            print(f"Throughput: {throughput:.2f} MB/s")
        print(f"{'='*60}\n")
        
        # Log results to CSV (even if there was an exception)
        status = "FAILED" if exc_type is not None else "SUCCESS"
        error_msg = str(exc_val) if exc_val is not None else ""
        
        self._log_result(status, error_msg)
        
        # Don't suppress exceptions
        return False
    
    def _log_result(self, status: str = "SUCCESS", error_msg: str = "") -> None:
        """
        Log benchmark results to CSV file.
        
        Args:
            status: Status of the benchmark (SUCCESS or FAILED)
            error_msg: Error message if the benchmark failed
            
        Raises:
            IOError: If writing to log file fails
        """
        # Check if log file exists to determine if we need to write headers
        file_exists = self.log_file.exists()
        
        try:
            # Create parent directory if needed
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Prepare row data
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row = {
                'timestamp': timestamp,
                'test_name': self.test_name,
                'description': self.description,
                'duration_seconds': f"{self.duration:.3f}" if self.duration else "",
                'data_size_mb': f"{self.data_size_mb:.2f}" if self.data_size_mb else "",
                'throughput_mbps': f"{self.data_size_mb/self.duration:.2f}" if self.data_size_mb and self.duration else "",
                'status': status,
                'error_message': error_msg
            }
            
            # Write to CSV
            with open(self.log_file, 'a', newline='', encoding='utf-8') as f:
                fieldnames = list(row.keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # Write header if file is new
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(row)
            
            print(f"✓ Results logged to: {self.log_file}")
            
        except IOError as e:
            print(f"⚠ Warning: Could not log results to {self.log_file}: {e}")


def get_directory_size_mb(directory_path: Path) -> float:
    """
    Calculate the total size of a directory in megabytes.
    
    Args:
        directory_path: Path to the directory to measure
        
    Returns:
        Total size in megabytes (MB)
        
    Raises:
        FileNotFoundError: If directory doesn't exist
        
    Example:
        >>> size = get_directory_size_mb(Path("data/processed/parquet"))
        >>> print(f"Directory size: {size:.2f} MB")
    """
    if not directory_path.exists():
        raise FileNotFoundError(f"Directory not found: {directory_path}")
    
    total_size = 0
    
    try:
        for item in directory_path.rglob('*'):
            if item.is_file():
                total_size += item.stat().st_size
    except Exception as e:
        print(f"⚠ Warning: Error calculating directory size: {e}")
        return 0.0
    
    # Convert bytes to megabytes
    return total_size / (1024 * 1024)


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to a human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "2m 30s" or "45.2s")
        
    Example:
        >>> format_duration(150.5)
        '2m 30s'
        >>> format_duration(45.2)
        '45.2s'
    """
    if seconds >= 60:
        minutes = int(seconds // 60)
        remaining_seconds = int(seconds % 60)
        return f"{minutes}m {remaining_seconds}s"
    else:
        return f"{seconds:.1f}s"


def print_benchmark_summary(log_file: Optional[Path] = None) -> None:
    """
    Print a summary of all benchmarks from the log file.
    
    Args:
        log_file: Optional path to log file (defaults to BENCHMARK_LOG_FILE)
        
    Example:
        >>> print_benchmark_summary()
    """
    log_path = log_file or BENCHMARK_LOG_FILE
    
    if not log_path.exists():
        print(f"⚠ No benchmark log found at: {log_path}")
        return
    
    print(f"\n{'='*80}")
    print(f"BENCHMARK SUMMARY")
    print(f"{'='*80}\n")
    
    try:
        with open(log_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            if not rows:
                print("No benchmark results found.")
                return
            
            # Print table header
            print(f"{'Test Name':<40} {'Duration':<15} {'Status':<10}")
            print(f"{'-'*40} {'-'*15} {'-'*10}")
            
            # Print each benchmark result
            for row in rows:
                test_name = row['test_name'][:40]
                duration = format_duration(float(row['duration_seconds'])) if row['duration_seconds'] else "N/A"
                status = row['status']
                
                print(f"{test_name:<40} {duration:<15} {status:<10}")
            
            # Print statistics
            successful = sum(1 for r in rows if r['status'] == 'SUCCESS')
            failed = len(rows) - successful
            total_time = sum(float(r['duration_seconds']) for r in rows if r['duration_seconds'])
            
            print(f"\n{'-'*80}")
            print(f"Total Tests: {len(rows)} | Successful: {successful} | Failed: {failed}")
            print(f"Total Time: {format_duration(total_time)}")
            print(f"{'='*80}\n")
            
    except Exception as e:
        print(f"⚠ Error reading benchmark log: {e}")


if __name__ == "__main__":
    # Demo usage
    print("BenchmarkTimer Demo\n")
    
    # Simulate a simple benchmark
    with BenchmarkTimer("Demo Test", description="Testing the timer") as timer:
        time.sleep(2)  # Simulate work
        
    print(f"\nMeasured duration: {timer.duration:.2f} seconds")
    
    # Print summary if logs exist
    print_benchmark_summary()
