2. Laufband (sequential API)
- Laufband should be implemented internally using Graphband, where the graph is a set of disconnected nodes (no edges / dependencies). This is realized by converting the generator into a graph_fn that consumes the generator output.
- tasks are discovered lazily.
- Each worker iterates the generator independently until it finds a new task not in DB.
- A task hash is computed for each yielded item to serve as the unique ID.
- Deduplication is handled centrally by the DB (atomic insert).
- Remove the constraint that laufband only supports fixed / known length input sequences.

3. Graphband (general DAG API)
- New class laufband.Graphband accepts a graph_fn: Callable[[], networkx.DiGraph] that returns the current graph state.
- Nodes = tasks, Edges = explicit dependencies.
- Iteration yields only ready nodes (all predecessors finished).
- Graph state is reevaluated before processing each new task, such that injecting new dependencies at runtime is possible.


4. Task identity
- By default, task IDs come from:
- hash(item) if the item is hashable and stable.
- Otherwise, a user-provided hash_fn(item) → str.
- IDs are persisted in DB as the primary key.
- Task identity must be deterministic across workers to prevent duplication.


5. Termination
- Iteration stops when:
    - No tasks are ready or in_progress, and
    - Input generator (if present) is exhausted,
