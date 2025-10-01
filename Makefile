.PHONY: install-poetry setup test test-debug lint doc clean

install-poetry:
	@command -v poetry >/dev/null 2>&1 && echo "✅ Poetry already installed." || \
		(curl -sSL https://install.python-poetry.org | python3 - && \
		echo "✅ Poetry installed. Add to PATH: export PATH=\"$$HOME/.local/bin:\$$PATH\"")

setup: install-poetry
	@echo "🐍 Setting up virtual environment..."
	poetry env use python3
	@echo "📦 Installing dependencies..."
	poetry install
	@echo "✅ Setup complete. Run 'poetry shell' to activate."

test:
	poetry run pytest --cov=pandaflow pandaflow

test-debug:
	poetry run pytest --cov=pandaflow pandaflow --pdb

lint:
	black pandaflow/
	poetry run flake8 pandaflow/ --count --select=E9,F63,F7,F82

doc:
	poetry run sphinx-build -b html docs/source docs/_build/html

# Clean artifacts
clean:
	rm -rf __pycache__ .pytest_cache .mypy_cache dist build "*.egg-info"
