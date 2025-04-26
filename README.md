[![codecov](https://codecov.io/gh/zincware/laufband/graph/badge.svg?token=9DJ3YZGTBA)](https://codecov.io/gh/zincware/laufband)
# Laufband: Parallel Iteration with File-Based Coordination

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