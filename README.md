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
from laufband import Laufband

data = list(range(100))
for item in Laufband(data):
    # Process each item in the dataset
    pass
```

The true power of Laufband emerges when you run your script in parallel. Multiple processes will coordinate using file-based locking to ensure that each item in the dataset is processed by only one process.

Here's a typical example demonstrating parallel processing with Laufband and file-based locking for shared resource access:

```python
import json
import time
from pathlib import Path
from laufband import Laufband

output_file = Path("data.json")
output_file.write_text(json.dumps({"processed_data": []}))
data = list(range(100))

worker = Laufband(data, desc="using Laufband")

for item in worker:
    # Simulate some computationally intensive task
    time.sleep(0.1)
    with worker.lock:
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

```python
from laufband import Laufband

data = list(range(100))

# Example 1: break
for item in Laufband(data):
    if item == 50:
        break  # Job 50 will be marked as failed

# Example 2: Exception
for item in Laufband(data):
    if item == 70:
        raise ValueError("Something went wrong")  # Job 70 will be marked as failed
```

If you want to exit early but still mark the job as successfully completed,
you should use `Laufband.close()` instead of `break`:

```python
from laufband import Laufband

data = list(range(100))

worker = Laufband(data)

for item in worker:
    if item == 50:
        worker.close()  # Job 50 will be marked as completed, and iteration will stop cleanly
```


# Examples

## ASE Calculator
For atomistic data, the ASE package is widely used to calculate energies and forces of atomic configurations using either _ab initio_ methods or machine-learned interatomic potentials (MLIPs).

You can use Laufband to parallelize these calculations easily without duplication or manual bookkeeping and automatic checkpointing.

The following example uses a MACE foundation model to compute energies and forces on the ASE S22 dataset.

> [!TIP]
> You can safely run this script multiple times — even across multiple SLURM jobs — without any modifications.
> Laufband will automatically coordinate which configurations are processed.
> For local parallelization, you can use bash: `for i in {1..10} ; do python main.py & done`

```python
import ase.io
from ase.collections import s22
from laufband import Laufband
from mace.calculators import mace_mp

# Initialize calculator
calc = mace_mp(model="medium", dispersion=False, default_dtype="float32")

worker = Laufband(list(s22))

for atoms in worker:
    atoms.calc = calc
    atoms.get_potential_energy()
    with worker.lock:
        ase.io.write("frames.xyz", atoms, append=True)
```