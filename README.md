# Laufband

With laufband you can iterate over a given dataset from multiple processes, using file-based locking and communication.

## Installation

```
pip install laufband
```

## Usage

You can use laufband like your normal tqdm progressbar:

```python
from laufband import laufband

data = list(range(100))
for point in laufband(data):
    # do something with data
```

The interesting part happens, if you run this program in parallel.
Each process will communicate with the others and ensure, that each item only appears in one of them.

Therefore, laufband uses locking. A typical example could look like this.

```python
import json
import time
from pathlib import Path

from flufl.lock import Lock
from laufband import laufband

output = Path("data.json")
output.write_text(json.dumps({"data": []}))
data = list(range(100))
lock = Lock("laufband.lock")

for point in laufband(data):
    # do some calculation you want to run in parallel
    time.sleep(0.1)
    with lock:
        # save the result to a file.
        # if it is a shared file, you need to use a lock
        filecontent = json.loads(output.read_text())
        filecontent["data"].append(point)
        output.write_text(json.dumps(filecontent))
```

Assuming this code is in a file `main.py` you can iterate in parallel.

```bash
for i in {1..10} ; do python main.py & done
```