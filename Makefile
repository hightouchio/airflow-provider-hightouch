.PHONY: build install clean

build:
	uv build

install:
	uv sync

clean:
	rm -rf dist
