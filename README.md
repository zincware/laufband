<div align="center">

![Logo](https://github.com/user-attachments/assets/c8d2a3f9-284b-474d-b46d-98612c9d266b)
</div>

# Laufband: Embarrassingly parallel, embarrassingly simple!
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

worker = Laufband(data, tqdm_kwargs={"desc": "using Laufband"})

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
    energy = atoms.get_potential_energy()
    worker.iterator.set_description(f"{energy = }")
    with worker.lock:
        ase.io.write("frames.xyz", atoms, append=True)
```

You can use the `laufband watch` to follow the progress across all active workers.

![Laufband CLI](https://github.com/user-attachments/assets/e3c8c345-994c-4b97-b3a3-7a14e460240b#gh-dark-mode-only "Laufband CLI")
![Laufband CLI](https://github.com/user-attachments/assets/1c07c641-add7-4c48-89f8-8da67a8061d1#gh-light-mode-only "Laufband CLI")


# Graphband

Laufband supports dependency-aware tasks through `laufband.Graphband`.
To utilize this functionality, you need to write an iterator taking care of the correct order of tasks.

> [!NOTE]
> The `laufband.Laufband` uses directed graph without any edges as input to `laufband.Graphband`.

```py
import networkx as nx
from laufband import Task

def graph_tasks():
    digraph = nx.DiGraph()
    edges = [
        ("a", "b"),
        ("a", "c"),
        ("b", "d"),
        ("b", "e"),
        ("c", "f"),
        ("c", "g"),
    ]
    digraph.add_edges_from(edges)
    for node in nx.topological_sort(digraph):
        yield Task(
            id=node,  # unique string representation of the task
            data=node, # optional data associated with the task
            dependencies=digraph.predecessors(node), # dependencies of the task
        )
```
Given this generator, you can iterate the graph in parallel using `laufband.Graphband`.

> [!WARNING]
> Once `laufband.Graphband` has executed a task, it will not re-execute it, even if a dependency is added later on.

```py
from laufband import Graphband

worker = Graphband(graph_task())

for task in worker:
    print(task.id, task.data)
```

# Labels, Requirements and Multiple Workers per Task

You can assign requirements to your tasks and labels to workers to control their execution.

```py
from laufband import Task, Graphband

def iterator():
    yield Task(id="task1")
    yield Task(id="task2", requirements={"gpu"})

w1 = Graphband(iterator(), identifier="w1")
w2 = Graphband(iterator(), identifier="w2", labels={"gpu"})

print([x.id for x in w1])
# [task1]
print([x.id for x in w2])
# [task2]
```

Sometimes, a task itself supports internal parallel execution, e.g. through nested use of `laufband` In such a case you can assign multiple workers to one task.

> [!NOTE]
> Keep in mind that `laufband` does not actually schedule the execution. The available workers per task depends on the number of workers being spawned.

```py
from laufband import Task, Graphband

def iterator():
    yield Task(id="task1", max_parallel_workers=2)
    # a maximum of 2 workers will be assign to this job until both successfully finish .

worker = Graphband(iterator())

for item in worker:
    # code that can be executed multiple times, e.g. via laufband itself.
```
