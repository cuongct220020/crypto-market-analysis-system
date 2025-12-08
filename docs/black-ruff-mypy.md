# Development Standards and Toolchain Guide

This reference document outlines the static analysis and code formatting workflows established for the Crypto Market Analysis System. It provides the necessary commands to maintain code quality, consistency, and type safety.

## Toolchain Overview

The project utilizes a suite of standard Python tools to enforce coding standards:

*   **Ruff**: A high-performance linter used to detect code smells, unused variables, and to automatically sort imports. It replaces legacy tools like Flake8 and isort.
*   **Black**: An uncompromising code formatter that ensures a consistent style across the codebase, adhering strictly to PEP 8 guidelines.
*   **Mypy**: A static type checker that analyzes type hints to prevent runtime type errors, enhancing code reliability.
*   **Pre-commit**: A framework that manages git hooks, automatically executing the above tools prior to any commit to ensure the repository remains in a clean state.

## Command Reference

The following commands are essential for daily development.

### Code Linting and Import Sorting
To identify potential errors and automatically correct fixable issues (including import sorting):
```bash
ruff check . --fix
```

### Code Formatting
To standardize code structure and spacing:
```bash
black .
```

### Static Type Checking
To verify type consistency and catch logical errors:
```bash
mypy .
```

### Automated Hook Execution
To manually trigger the pre-commit pipeline on all files (useful for verifying changes before staging):
```bash
pre-commit run --all-files
```

## Workflow Integration

1.  **Initialization**: Ensure the pre-commit hooks are installed locally by running `pre-commit install` once.
2.  **Routine**: It is recommended to run `ruff` and `black` periodically during development.
3.  **Validation**: `mypy` should be consulted to resolve type ambiguities, particularly when modifying core data structures.
