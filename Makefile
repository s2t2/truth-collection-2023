
pip:
	pip install -r requirements.txt

pytest:
	pytest --disable-pytest-warnings


bq:
	python -m app.bq_service

ts:
	python -m app.truth_service
