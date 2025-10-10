# AGENTS.md

## Build/Lint/Test Commands

```bash
# Install dependencies
pip install -e .

# Run all tests
pytest

# Run single test file
pytest tests/test_converter.py

# Run with coverage
pytest --cov=sql2sparql

# Lint code
flake8 sql2sparql/

# Format code
black sql2sparql/

# Type checking
mypy sql2sparql/
```

## Code Style Guidelines

- **Imports**: Use absolute imports, group standard library, third-party, then local imports
- **Formatting**: Follow Black formatting with 88-character line length
- **Types**: Use type hints consistently, prefer `dataclasses` for models
- **Naming**: snake_case for variables/functions, PascalCase for classes, UPPER_CASE for constants
- **Error Handling**: Use specific exceptions, include descriptive error messages
- **Documentation**: Docstrings for all public classes and functions using Google style
- **Testing**: Write comprehensive tests with pytest, include edge cases and error conditions