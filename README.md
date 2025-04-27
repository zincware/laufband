<div align="center">
    
![Logo](https://github.com/user-attachments/assets/c8d2a3f9-284b-474d-b46d-98612c9d266b)
</div>

# Laufband: Parallel Iteration with File-Based Coordination
[![codecov](https://codecov.io/gh/zincware/laufband/graph/badge.svg?token=9DJ3YZGTBA)](https://codecov.io/gh/zincware/laufband)

Laufband enables parallel iteration over a dataset from multiple processes, utilizing file-based locking and communication to ensure each item is processed exactly once.

## Installation

Install Laufband using pip:

```bash
pip install laufband
```

## Usage

Using Laufband is similar to the familiar `tqdm` progress bar for sequential iteration.

```python
from laufband import laufband

data = list(range(100))
for item in laufband(data):
    # Process each item in the dataset
    pass
```

The true power of Laufband emerges when you run your script in parallel. Multiple processes will coordinate using file-based locking to ensure that each item in the dataset is processed by only one process.

Here's a typical example demonstrating parallel processing with Laufband and file-based locking for shared resource access:

```python
import json
import time
from pathlib import Path
from flufl.lock import Lock
from laufband import laufband

output_file = Path("data.json")
output_file.write_text(json.dumps({"processed_data": []}))
data = list(range(100))
lock = Lock("laufband.lock")

for item in laufband(data, lock=lock, desc="using Laufband"):
    # Simulate some computationally intensive task
    time.sleep(0.1)
    with lock:
        # Access and modify a shared resource (e.g., a file) safely using the lock
        file_content = json.loads(output_file.read_text())
        file_content["processed_data"].append(item)
        output_file.write_text(json.dumps(file_content))
```

To execute this script (`main.py`) in parallel, you can use a command like the following in your terminal (this example launches 10 background processes):

```bash
for i in {1..10} ; do python main.py & done
```

> [!IMPORTANT]
> The different processes may finish at different times. Therefore, the order of items in `file_content` is not guaranteed.
> If the order is important, you will need to implement sorting logic afterwards.

### Failure Policy

In Laufband, a job will be automatically marked as failed if the iteration is interrupted by:
- an unhandled Exception
- or an explicit break.

Examples:
```python
from laufband import laufband

data = list(range(100))

# Example 1: break
for item in laufband(data):
    if item == 50:
        break  # Job 50 will be marked as failed

# Example 2: Exception
for item in laufband(data):
    if item == 70:
        raise ValueError("Something went wrong")  # Job 70 will be marked as failed
```

If you want to exit early but still mark the job as successfully completed,
you should use laufband.close() instead of break:

```python
from laufband import laufband, close

data = list(range(100))

for item in laufband(data):
    if item == 50:
        close()  # Job 50 will be marked as completed, and iteration will stop cleanly
```
