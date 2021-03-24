import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="airflow-provider-hightouch",
    version="1.0.0",
    author="Great Expectations",
    description="An Apache Airflow provider for Hightouch.io",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hightouch/airflow-provider-hightouch",
    packages=setuptools.find_packages(),
    python_requires=">=3.6",
    include_package_data=True,
)
