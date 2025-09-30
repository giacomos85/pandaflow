test:
	poetry run pytest --cov=pandaflow pandaflow

lint:
	black pandaflow/
	poetry run flake8 pandaflow/ --count --select=E9,F63,F7,F82

doc:
	poetry run sphinx-build -b html docs/source docs/_build/html