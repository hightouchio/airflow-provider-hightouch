import os
import sys

from setuptools.config import read_configuration


def get_version():
    cfg = read_configuration("setup.cfg")
    version = cfg["metadata"]["version"]
    return version


def validate_version():
    version = get_version()
    tag = os.getenv("CIRCLE_TAG")
    if tag != version:
        info = "Git tag: {0} does not match the version : {1}".format(tag, version)
        sys.exit(info)


if __name__ == "__main__":
    validate_version()
