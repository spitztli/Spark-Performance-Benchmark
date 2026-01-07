# Hadoop Windows Fix for Spark CSV Write Error

## Problem
You encountered this error when writing CSV files:
```
java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
```

## Solution Implemented

### ✅ Step 1: Hadoop Binaries (COMPLETED)
You already have the required files in `C:\hadoop\bin\`:
- ✅ `winutils.exe`
- ✅ `hadoop.dll`

### ✅ Step 2: Updated config.py (COMPLETED)
I've added a `configure_hadoop_home()` function to `src/config.py` that automatically configures the HADOOP_HOME environment variable.

### 🔧 Step 3: Update Your Notebook (YOU NEED TO DO THIS)

**Add this code cell to your notebook BEFORE the Spark session is created:**

```python
# Configure Hadoop for Windows (MUST run before creating SparkSession)
from config import configure_hadoop_home
configure_hadoop_home()
```

**Where to add it:**
- After the cell that imports from config
- BEFORE the cell that creates `SparkSession.builder`

**Example order:**
1. Cell: Import libraries and config ✓
2. **NEW CELL: Call configure_hadoop_home()** ← ADD THIS
3. Cell: Create Spark session ✓

### Step 4: Restart Jupyter Kernel
After adding the new cell:
1. In Jupyter, click: **Kernel** → **Restart Kernel**
2. Re-run all cells from the beginning

---

## Alternative: Manual HADOOP_HOME Setup (Optional)

If you want to set HADOOP_HOME permanently in Windows (so it works even if you forget to call the function):

### Option A: Windows GUI
1. Press `Win + X` → Select "System"
2. Click "Advanced system settings"
3. Click "Environment Variables"
4. Under "System variables", click "New"
5. Variable name: `HADOOP_HOME`
6. Variable value: `C:\hadoop`
7. Click OK, restart VS Code/Jupyter

### Option B: PowerShell (Administrator)
```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "Machine")
```
Then restart your terminal and Jupyter.

---

## Testing

After making the changes, run this in a Python cell to verify:

```python
import os
print("HADOOP_HOME:", os.environ.get('HADOOP_HOME'))

# Should show:
# HADOOP_HOME: C:\hadoop
```

---

## What's Fixed?

The `configure_hadoop_home()` function:
1. ✅ Checks if HADOOP_HOME is already set
2. ✅ Sets it to `C:\hadoop` if not set
3. ✅ Verifies that `winutils.exe` and `hadoop.dll` exist
4. ✅ Prints confirmation messages
5. ✅ Must run BEFORE SparkSession creation

---

## Summary

**What you need to do NOW:**
1. Open your notebook: `01_data_generation_paysim-5.ipynb`
2. Find the cell that creates SparkSession (starts with `spark = SparkSession.builder`)
3. Insert a NEW cell ABOVE it with:
   ```python
   from config import configure_hadoop_home
   configure_hadoop_home()
   ```
4. Restart kernel and re-run all cells
5. The CSV write error should be gone! ✅
