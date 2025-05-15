# Contributing to TestDataPy

Thank you for your interest in contributing to TestDataPy! This document provides guidelines and instructions for contributing to the project.

## Table of Contents
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Reporting Issues](#reporting-issues)

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/testdatapy.git
   cd testdatapy
   ```
3. Create a new branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Setup

### Prerequisites
- Python 3.11 or higher
- UV package manager (recommended) or pip
- Docker and Docker Compose for integration tests
- Git

### Setting Up Your Environment

1. Install UV (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Install development dependencies:
   ```bash
   uv pip install -e .[dev]
   ```

3. Set up pre-commit hooks:
   ```bash
   make dev-setup
   ```

4. Start the local Kafka environment:
   ```bash
   make docker-up
   ```

## Making Changes

### Code Organization

The project follows this structure:
```
src/testdatapy/
├── generators/     # Data generation modules
├── producers/      # Kafka producer implementations
├── config/         # Configuration management
├── schemas/        # Avro schemas
└── cli.py         # Command-line interface
```

### Adding a New Generator

1. Create a new file in `src/testdatapy/generators/`
2. Inherit from `DataGenerator` base class
3. Implement the `generate()` method
4. Add tests in `tests/unit/test_your_generator.py`
5. Update `__init__.py` to export your generator

Example:
```python
from testdatapy.generators.base import DataGenerator

class MyGenerator(DataGenerator):
    def generate(self):
        while self.should_continue():
            data = {"id": self.message_count}
            yield data
            self.increment_count()
```

### Adding a New Producer

1. Create a new file in `src/testdatapy/producers/`
2. Inherit from `KafkaProducer` base class
3. Implement required methods
4. Add tests in `tests/unit/test_your_producer.py`
5. Update `__init__.py` to export your producer

### Adding CLI Commands

1. Edit `src/testdatapy/cli.py`
2. Use Click decorators to define commands
3. Add tests in `tests/unit/test_cli.py`

## Testing

### Running Tests

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests
make test-integration

# Run with coverage
make test-with-coverage
```

### Writing Tests

#### Unit Tests
- Place in `tests/unit/`
- Mock external dependencies
- Test individual components in isolation
- Aim for 90%+ coverage

Example:
```python
def test_my_generator():
    generator = MyGenerator(max_messages=5)
    messages = list(generator.generate())
    assert len(messages) == 5
```

#### Integration Tests
- Place in `tests/integration/`
- Test with real Kafka and Schema Registry
- Use Docker Compose environment
- Test end-to-end workflows

### Test Coverage

We aim for 90%+ test coverage. Check coverage with:
```bash
make coverage
```

## Code Style

### Standards
- Follow PEP 8
- Use type hints
- Write docstrings for all public functions
- Keep functions focused and small

### Linting and Formatting

The project uses:
- Black for code formatting
- Ruff for linting
- MyPy for type checking

Run all checks:
```bash
make check
```

Format code:
```bash
make format
```

### Docstring Format

Use Google-style docstrings:
```python
def my_function(param1: str, param2: int) -> bool:
    """Brief description of function.
    
    Longer description if needed.
    
    Args:
        param1: Description of param1
        param2: Description of param2
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: When something is wrong
    """
```

## Submitting Changes

### Pull Request Process

1. Ensure all tests pass:
   ```bash
   make check
   ```

2. Update documentation if needed:
   - README.md for user-facing changes
   - API_REFERENCE.md for API changes
   - USER_GUIDE.md for new features

3. Write a clear commit message:
   ```
   feat: Add support for Protobuf format
   
   - Implement ProtobufProducer class
   - Add protobuf dependency
   - Update CLI to support --format protobuf
   
   Closes #123
   ```

4. Push to your fork and create a pull request

### Commit Message Format

Follow conventional commits:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `style:` Code style changes (formatting, etc.)
- `refactor:` Code refactoring
- `test:` Adding or updating tests
- `chore:` Maintenance tasks

### Pull Request Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Added new tests for changes

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review performed
- [ ] Documentation updated
- [ ] No breaking changes
```

## Reporting Issues

### Bug Reports

Include:
1. TestDataPy version
2. Python version
3. Operating system
4. Kafka/Schema Registry versions
5. Steps to reproduce
6. Expected behavior
7. Actual behavior
8. Error messages/stack traces

### Feature Requests

Include:
1. Use case description
2. Proposed solution
3. Alternative solutions considered
4. Additional context

### Security Issues

For security vulnerabilities:
1. DO NOT open a public issue
2. Email security@testdatapy.com
3. Include detailed description
4. Wait for acknowledgment

## Code of Conduct

### Our Standards
- Be respectful and inclusive
- Welcome newcomers
- Accept constructive criticism
- Focus on what's best for the community

### Unacceptable Behavior
- Harassment or discrimination
- Trolling or insulting comments
- Public or private harassment
- Publishing private information

## Getting Help

- Read the documentation
- Search existing issues
- Ask in discussions
- Join our Slack channel

## Recognition

Contributors are recognized in:
- AUTHORS.md file
- Release notes
- Project documentation

Thank you for contributing to TestDataPy!
