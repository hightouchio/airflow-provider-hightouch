def get_provider_info():
    return {
        "package-name": "airflow-provider-hightouch",
        "name": "Hightouch Provider",
        "description": "Hightouch API hooks for Airflow <https://hightouch.io/>",
        "versions": ["1.0.1"],
        "hook-class-names": [
            "airflow_provider_hightouch.hooks.hightouch.HightouchHook"
        ],
        "extra-links": ["airflow_provider_hightouch.operators.hightouch.HightouchLink"],
    }
