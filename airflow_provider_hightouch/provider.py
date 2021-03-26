def get_provider_info():
    return {
        "package-name": "airflow-provider-hightouch",
        "name": "Hightouch",
        "description": "Hightouch Airflow Provider <https://hightouch.io/>",
        "versions": ["1.0.0"],
        "extra-links": [
            "airflow_provider_hightouch.operators.hightouch.HightouchTriggerSyncOperator"
        ],
    }
