"""
Test script to verify Hadoop configuration for Windows.

Run this script to verify that HADOOP_HOME is properly configured
before running your Spark notebooks.

Usage:
    python test_hadoop_config.py
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config import configure_hadoop_home

print("=" * 70)
print("HADOOP CONFIGURATION TEST")
print("=" * 70)
print()

# Test the configuration function
try:
    configure_hadoop_home()
    print()
    print("=" * 70)
    print("✅ SUCCESS: Hadoop configuration completed!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("1. Add the following to your notebook BEFORE creating SparkSession:")
    print()
    print("   from config import configure_hadoop_home")
    print("   configure_hadoop_home()")
    print()
    print("2. Restart your Jupyter kernel")
    print("3. Re-run all cells")
    print("4. CSV write operations should now work! ✅")
    print()
    print("=" * 70)
    
except FileNotFoundError as e:
    print()
    print("=" * 70)
    print("❌ ERROR: Missing Hadoop files")
    print("=" * 70)
    print()
    print(str(e))
    print()
    print("Please ensure you have:")
    print("- C:\\hadoop\\bin\\winutils.exe")
    print("- C:\\hadoop\\bin\\hadoop.dll")
    print()
    sys.exit(1)
    
except Exception as e:
    print()
    print("=" * 70)
    print("❌ ERROR: Unexpected error")
    print("=" * 70)
    print()
    print(str(e))
    print()
    sys.exit(1)
