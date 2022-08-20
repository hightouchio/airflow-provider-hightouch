from .version import __version__


def get_provider_info():
    return {
        "package-name": "airflow-provider-hightouch",
        "name": "Hightouch Provider",
        "description": "Hightouch API hooks for Airflow <https://hightouch.io/>",
        "versions": __version__,
        "extra-links": ["airflow_provider_hightouch.operators.hightouch.HightouchLink"],
    }
