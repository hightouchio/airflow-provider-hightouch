## Developing

### Prerequisities

You will need Airflow installed. One way to do this is to use 
[pyenv](https://github.com/pyenv/pyenv) and 
[pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) to create a virtual environment.



```
mkdir -p ~/airflow
cd ~/airflow
pyenv virtualenv airflow
pyenv local airflow
pip install apache-airflow
```

Once installed, you can install this pacakge locally.

```
cd ~/projects
git clone git@github.com:hightouchio/airflow-provider-hightouch.git
cd airflow-provider-hightouch

# Activate airflow venv
pyenv local airflow
pip install -e .
```

Next, spin up Airflow. Make sure to set AIRFLOW_HOME to the directory you used
for your virtual environment, then copy the example dag over and start Airflow 
test server

```
cd ~/airflow
export AIRFLOW_HOME=~/airflow

# Don't load example dags by default
export AIRFLOW__CORE__LOAD_EXAMPLES=false 

# Copy the example dag over
mkdir -p ~/airflow/dags
cp ~/projects/airflow-provider-hightouch/airflow_provider_hightouch/example_dags/example_hightouch_trigger_sync.py ~/airflow/dags

# https://github.com/apache/airflow/issues/12808
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
airflow standalone
```

Once complete, you'll see the admin password in the console. Use that to login at http://localhost:8080
and continue the setup using the instructions in README.md to set an API key, connection etc.

## Releasing

Update the version in `airflow_provider_hightouch/version.py`
Add details in CHANGELOG.md
Once the changes have been merged to main, tag the release and the deploy will complete through CircleCI