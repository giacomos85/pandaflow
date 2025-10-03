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
	rm -rf docs/_build
	cd docs/ && python generate_strategy_docs.py
	poetry run sphinx-build -b html docs/source docs/_build/html
	pandaflow dump --config --output docs/source/pandaflow-schema.json

doc-serve:
	python -m http.server 8080 --directory docs/_build/html/

# Clean artifacts
clean:
	rm -rf __pycache__ .pytest_cache .mypy_cache dist build "*.egg-info"


demo-%:  ## Run strategy demo for a given strategy name (e.g., make demo-replace)
	@echo "🔍 Running demo for strategy: $*"
	@poetry run pandaflow run \
		--config docs/source/data/$*/pandaflow-config.json \
		--input docs/source/data/$*/input.csv \
		--output tmp/output-$*.csv
	@echo "✅ Demo completed for $*"

validate-%:  demo-%
	@echo "🔎 Validating output for strategy: $*"
	@diff -u docs/source/data/$*/output.csv tmp/output-$*.csv || echo "⚠️ Differences found in $*"

SKIP_STRATEGIES := uuid debug

validate-all:  ## Run all strategy demos
	@for s in $(shell ls docs/source/data); do \
		if ! echo "$(SKIP_STRATEGIES)" | grep -qw "$$s"; then \
            $(MAKE) validate-$$s; \
        else \
            echo "⏭️ Skipping $$s"; \
        fi; \
	done