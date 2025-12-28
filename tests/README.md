# ETL Framework Testing Suite

Comprehensive testing framework for the metadata-driven ETL framework with unit tests, integration tests, data validation tests, performance tests, and mock data generation.

## Test Structure

```
tests/
├── __init__.py
├── unit_tests.py              # Unit tests for individual components
├── integration_tests.py       # Integration tests for component interactions
├── data_validation_tests.py   # Data quality and validation tests
├── performance_tests.py       # Performance and scalability tests
├── mock_data_generator.py     # Mock data generation utilities
├── test_utilities.py          # Common test utilities and fixtures
└── README.md                  # This file
```

## Test Categories

### 1. Unit Tests (`unit_tests.py`)

Tests individual components in isolation:

- **ConnectorFactory**: Connection creation and management
- **TransformEngine**: Data transformation logic
- **EngineSelector**: Engine selection algorithms
- **DQEngine**: Data quality validation rules
- **AnomalyDetection**: Anomaly detection methods
- **SLAMonitor**: SLA compliance checks

### 2. Integration Tests (`integration_tests.py`)

Tests component interactions and end-to-end flows:

- **Extract-Transform Flow**: E-T integration testing
- **Transform-Load Flow**: T-L integration testing
- **End-to-End Pipeline**: Complete pipeline execution
- **DQ Integration**: Data quality in pipeline context
- **Error Handling Flow**: Error propagation and recovery

### 3. Data Validation Tests (`data_validation_tests.py`)

Tests data quality, schema validation, and business rules:

- **Schema Validation**: Schema conformance checking
- **Data Type Validation**: Type correctness validation
- **Referential Integrity**: Foreign key validation
- **Business Rules**: Domain-specific rule validation
- **Data Completeness**: Null value and completeness checks

### 4. Performance Tests (`performance_tests.py`)

Tests scalability and performance characteristics:

- **Small Dataset Performance**: < 1GB datasets
- **Medium Dataset Performance**: 1-10GB datasets
- **Large Dataset Performance**: > 10GB datasets
- **Concurrent Execution**: Parallel pipeline runs
- **Memory Usage**: Resource consumption monitoring

### 5. Mock Data Generator (`mock_data_generator.py`)

Generates test data for various scenarios:

- **Valid Test Data**: Schema-compliant data generation
- **Invalid Test Data**: Data with intentional errors
- **Edge Case Data**: Boundary condition data
- **Performance Test Data**: Large-scale test datasets

## Test Utilities (`test_utilities.py`)

Common utilities for testing:

- **Test Database Management**: Setup/teardown test databases
- **Mock Objects**: Database connections and component mocks
- **Data Comparison**: DataFrame equality assertions
- **Performance Measurement**: Execution time and memory monitoring
- **Error Simulation**: Controlled error injection
- **Test Fixtures**: Pytest fixtures for common test data

## Running Tests

### Prerequisites

```bash
pip install -r requirements.txt
pip install pytest pytest-cov pytest-mock
```

### Run All Tests

```bash
# Using the test runner
python run_tests.py all

# Or directly with pytest
pytest tests/
```

### Run Specific Test Categories

```bash
# Unit tests only
python run_tests.py unit

# Integration tests only
python run_tests.py integration

# Data validation tests
python run_tests.py data_validation

# Performance tests
python run_tests.py performance

# Slow tests only
python run_tests.py slow
```

### Run Tests with Coverage

```bash
python run_tests.py coverage
```

This generates HTML coverage reports in `htmlcov/`.

### Run Individual Test Files

```bash
pytest tests/unit_tests.py -v
pytest tests/integration_tests.py -v
pytest tests/data_validation_tests.py -v
pytest tests/performance_tests.py -v
```

## Test Configuration

Tests are configured via `pytest.ini`:

```ini
[tool:pytest]
testpaths = tests
python_files = *_tests.py
python_classes = Test*
python_functions = test_*
addopts =
    --verbose
    --tb=short
    --strict-markers
    --disable-warnings
    --cov=src
    --cov-report=html:htmlcov
    --cov-report=term-missing
    --cov-fail-under=80
```

## Test Markers

- `@pytest.mark.unit`: Unit tests
- `@pytest.mark.integration`: Integration tests
- `@pytest.mark.performance`: Performance tests
- `@pytest.mark.slow`: Slow-running tests

Skip slow tests:
```bash
pytest -m "not slow"
```

Run only performance tests:
```bash
pytest -m "performance"
```

## Mock Data Generation

### Basic Usage

```python
from tests.mock_data_generator import MockDataGenerator, TestDataFactory

# Create generator
generator = MockDataGenerator(seed=42)

# Generate valid data
schema = TestDataFactory.create_employee_schema()
valid_data = generator.generate_valid_test_data(schema, 100)

# Generate invalid data for testing error handling
invalid_data = generator.generate_invalid_test_data(schema, 50, ['nulls', 'wrong_types'])

# Generate edge cases
edge_cases = generator.generate_edge_case_data(schema, ['null_value', 'empty_string', 'maximum_value'])
```

### Convenience Functions

```python
from tests.mock_data_generator import generate_valid_test_data, generate_invalid_test_data

# Quick data generation
employees = generate_valid_test_data(TestDataFactory.create_employee_schema(), 100)
invalid_employees = generate_invalid_test_data(TestDataFactory.create_employee_schema(), 20)
```

## Test Utilities Usage

### Database Testing

```python
from tests.test_utilities import TestDatabaseManager

# Setup test database
manager = TestDatabaseManager()
db_utils = manager.setup_test_database("test_db")

# Load test metadata
config = create_test_pipeline_config()
manager.load_test_metadata(db_utils, config)

# Cleanup
manager.teardown_test_database("test_db")
```

### Data Comparison

```python
from tests.test_utilities import assert_data_equality

# Compare DataFrames
assert_data_equality(expected_df, actual_df, ignore_columns=['timestamp'])
```

### Performance Measurement

```python
from tests.test_utilities import measure_execution_time

# Measure function execution
result, exec_time = measure_execution_time(my_function, arg1, arg2)
print(f"Function took {exec_time:.2f} seconds")
```

### Error Simulation

```python
from tests.test_utilities import simulate_error

# Simulate specific errors
try:
    raise simulate_error('connection_error')
except ConnectionError:
    # Handle connection error
    pass
```

## Test Data Factories

Pre-built factories for common test scenarios:

```python
from tests.test_utilities import EmployeeDataFactory, SalesDataFactory

# Generate test employees
employees = EmployeeDataFactory.create_valid_employees(50)
invalid_employees = EmployeeDataFactory.create_invalid_employees(10)

# Generate test sales data
sales = SalesDataFactory.create_valid_sales(100)
high_value_sales = SalesDataFactory.create_high_value_sales(25)
```

## Pytest Fixtures

Common fixtures available in all tests:

```python
def test_my_function(test_db_manager, sample_employee_data, test_pipeline_config):
    # test_db_manager: Database manager for test databases
    # sample_employee_data: Sample employee DataFrame
    # test_pipeline_config: Sample pipeline configuration
    pass
```

## Writing New Tests

### Unit Test Example

```python
import unittest
from tests.test_utilities import TestDatabaseManager

class TestMyComponent(unittest.TestCase):
    def setUp(self):
        self.db_manager = TestDatabaseManager()
        self.db_utils = self.db_manager.setup_test_database()

    def tearDown(self):
        self.db_manager.teardown_test_database("test_db")

    def test_my_function(self):
        # Test implementation
        result = my_function(self.db_utils)
        self.assertIsNotNone(result)
```

### Integration Test Example

```python
import pytest

@pytest.mark.integration
def test_component_integration(test_db_manager, sample_employee_data):
    # Setup components
    db_utils = test_db_manager.setup_test_database()

    # Test interaction between components
    # ... test code ...

    # Cleanup happens automatically via fixture
```

### Performance Test Example

```python
import pytest
from tests.test_utilities import measure_execution_time

@pytest.mark.performance
@pytest.mark.slow
def test_large_dataset_performance():
    # Generate large test dataset
    large_data = generate_performance_test_data(0.5)  # 500MB

    # Measure performance
    result, exec_time = measure_execution_time(process_data, large_data)

    # Assert performance requirements
    assert exec_time < 30.0  # Must complete in 30 seconds
```

## Continuous Integration

For CI/CD pipelines, use:

```bash
# Run all tests with coverage
python run_tests.py coverage

# Fail if coverage below 80%
# Fail if any tests fail
```

## Test Reporting

- **Coverage Reports**: Generated in `htmlcov/` directory
- **Test Results**: Standard pytest output
- **Performance Metrics**: Custom timing reports in performance tests

## Best Practices

1. **Isolation**: Each test should be independent
2. **Mock External Dependencies**: Use mocks for databases, APIs, etc.
3. **Test Data**: Use generated test data, not production data
4. **Performance Baselines**: Establish and monitor performance baselines
5. **Edge Cases**: Test boundary conditions and edge cases
6. **Error Scenarios**: Test error handling and recovery paths

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure `PYTHONPATH` includes `src/` directory
2. **Database Errors**: Check test database setup and cleanup
3. **Memory Issues**: Large performance tests may need more memory
4. **Slow Tests**: Use `@pytest.mark.slow` and skip in fast CI runs

### Debug Mode

Run tests with detailed output:
```bash
pytest tests/ -v -s --tb=long
```

### Profiling Tests

Profile slow tests:
```bash
python -m cProfile -s time $(which pytest) tests/performance_tests.py::TestPerformance::test_large_dataset -v
```</content>
<parameter name="filePath">d:\development\2026\metadata-etl-framework\tests\README.md