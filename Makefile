.PHONY: build install

build:
	python3 -m build


install: build
	pip3 install -e .

clean:
	rm -rf airflow_provider_hightouch.egg-info
	rm -rf build
	rm -rf dist
