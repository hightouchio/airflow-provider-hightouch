import os
import sys

__version__ = "2.1.1"


def validate_version():
    version = __version__
    tag = os.getenv("CIRCLE_TAG")
    if tag != version:
        info = "Git tag: {0} does not match the version : {1}".format(tag, version)
        sys.exit(info)


if __name__ == "__main__":
    validate_version()
