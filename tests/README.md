# Unit Tests for SC Topic Modelling Worker

This directory contains comprehensive unit tests for the sc-topic-modelling-worker project.

## Test Coverage

The test suite provides unit test coverage for the following classes and components:

### Core Classes
- **Supervisor class** (`supervisor.py`) - Worker process management, health monitoring, message routing
- **Worker base class** (`workers/Worker.py`) - Abstract base class interface

### Worker Implementations
- **PreprocessingWorker** (`workers/PreprocessingWorker.py`) - Text preprocessing with Azure OpenAI
- **ETMWorker** (`workers/ETMWorker.py`) - Embedded Topic Modeling using OCTIS
- **LLMWorker** (`workers/LLMWorker.py`) - Language model operations with Azure OpenAI
- **RabbitMQWorker** (`workers/RabbitMQWorker.py`) - RabbitMQ message queue integration
- **DatabaseInteractionWorker** (`workers/DatabaseInteractionWorker.py`) - MongoDB database operations

## Test Files

- `test_worker.py` - Tests for the abstract Worker base class
- `test_comprehensive_patterns.py` - Comprehensive tests for all worker patterns and logic
- `run_tests.py` - Test runner script

## Running Tests

### Run All Tests
```bash
python tests/run_tests.py
```

### Run Individual Test Files
```bash
# Test Worker base class
python -m unittest tests.test_worker -v

# Test comprehensive patterns
python -m unittest tests.test_comprehensive_patterns -v
```

### Run Specific Test Classes
```bash
# Test supervisor logic patterns
python -m unittest tests.test_comprehensive_patterns.TestSupervisorLogic -v

# Test worker patterns
python -m unittest tests.test_comprehensive_patterns.TestWorkerPatterns -v

# Test worker-specific logic
python -m unittest tests.test_comprehensive_patterns.TestWorkerSpecificLogic -v
```

## Test Design Philosophy

The tests are designed with the following principles:

1. **Dependency Independence**: Tests focus on logic patterns rather than external dependencies
2. **Mocking Strategy**: External services (Azure OpenAI, MongoDB, RabbitMQ) are mocked
3. **Pattern Testing**: Tests verify the correct implementation of common patterns
4. **Logic Verification**: Core business logic and data flow are thoroughly tested
5. **Interface Compliance**: Abstract base class implementations are verified

## Test Categories

### 1. Supervisor Logic Tests
- Worker creation and management
- Health check mechanisms
- Message routing and handling
- Process lifecycle management
- Pending message tracking

### 2. Worker Pattern Tests
- Initialization patterns
- Configuration handling
- Health check messaging
- Async task patterns
- Threading patterns

### 3. Worker-Specific Tests
- Azure OpenAI configuration (PreprocessingWorker, LLMWorker)
- Dataset configuration (ETMWorker)
- Queue configuration (RabbitMQWorker)
- Database configuration (DatabaseInteractionWorker)

### 4. Message Handling Tests
- Message conversion and routing
- Error handling patterns
- Communication patterns
- Multiprocessing patterns

## Dependencies

The tests use Python's built-in `unittest` framework and `unittest.mock` for mocking.

No external test dependencies are required.

## Expected Output

When all tests pass, you should see:
```
✅ ALL TESTS PASSED!
Ran XX tests successfully
```

The test suite covers critical functionality including:
- Worker lifecycle management
- Inter-process communication
- Health monitoring
- Configuration management
- Error handling
- Message routing and processing