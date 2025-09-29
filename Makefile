test:
	poetry run pytest --cov=pandaflow pandaflow --pdb

lint:
	black pandaflow/
	poetry run flake8 pandaflow/ --count --select=E9,F63,F7,F82