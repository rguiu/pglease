.PHONY: help install test lint format clean build publish

help:
	@echo "Coor - Development Commands"
	@echo ""
	@echo "install       Install development dependencies"
	@echo "test          Run tests"
	@echo "test-cov      Run tests with coverage report"
	@echo "lint          Run linting checks"
	@echo "format        Format code with black"
	@echo "type-check    Run type checking with mypy"
	@echo "clean         Remove build artifacts"
	@echo "build         Build distribution packages"
	@echo "publish       Publish to PyPI"

install:
	pip install -e ".[dev]"

test:
	pytest -v

test-cov:
	pytest --cov=coor --cov-report=html --cov-report=term

lint:
	ruff check src/ tests/
	black --check src/ tests/

format:
	black src/ tests/
	ruff check --fix src/ tests/

type-check:
	mypy src/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

publish: build
	python -m twine upload dist/*

docker-postgres:
	docker run -d \
		--name coor-test-db \
		-e POSTGRES_USER=test \
		-e POSTGRES_PASSWORD=test \
		-e POSTGRES_DB=coor_test \
		-p 5433:5432 \
		postgres:14
	@echo "PostgreSQL running on port 5433"
	@echo "Connection string: postgresql://test:test@localhost:5433/coor_test"
	@echo "Set environment variable:"
	@echo "  export TEST_POSTGRES_URL=\"postgresql://test:test@localhost:5433/coor_test\""

docker-stop:
	docker stop coor-test-db
	docker rm coor-test-db
