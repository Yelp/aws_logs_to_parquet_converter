.PHONY: clean
clean:
	rm -rf venv

.PHONY: venv
venv:
	virtualenv -p python3.6 venv

requirements: requirements.txt requirements-dev.txt
	. venv/bin/activate && pip install -r requirements.txt -r requirements-dev.txt
