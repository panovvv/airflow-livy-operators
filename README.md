TODO UPDATE README

# Airflow Livy Plugins

[![Build Status](https://travis-ci.org/panovvv/airflow-livy-plugins.svg?branch=master)](https://travis-ci.org/panovvv/airflow-livy-plugins)
[![Code coverage](https://codecov.io/gh/panovvv/airflow-livy-plugins/branch/master/graph/badge.svg)](https://codecov.io/gh/panovvv/airflow-livy-plugins)

Plugins for Airflow to run Spark jobs via Livy: session mode, batch mode and hybrid (batch + Spark REST API)

See [this blog post](https://www.shortn0tes.com/2019/08/airflow-spark-livy-sessions-batches.html "Blog post") for more information and detailed comparison of ways to run Spark jobs from Airflow.

## Folder structure
* `airflow`: DAGs and plugins for Airflow. Those 2 directories correspond
to Airflow folders of the same name.
* `pyspark`: Spark jobs code.

## How to...


### ...run the examples?
Prerequisites:
* Python 3. Make sure it's installed and in __$PATH__
* Docker, Docker-Compose.
* You also need to set a specific environment variable for Airflow to work:
```bash
export SLUGIFY_USES_TEXT_UNIDECODE=yes
```
You may add this line to your __.bashrc__/__.bash_profile__ file if you're running a
UNIX-compliant OS.


Now, 
* run `make up` in this directory to bring up the whole infrastructure. 
Airflow UI will be available at
[localhost:8080](http://localhost:8080 "Airflow UI").

* `make logs` will follow Airflow logs.

* `make down` to dispose of Airflow (remove all Airflow-related Docker
containers)

* Look at the Makefile in repository root to find out what other commands
are available.


### ...set up development environment?

* run `make venv` in this directory.
* (Pycharm-specific) point PyCharm to your newly-created virtual environment: go to
"Preferences" -> "Project: airflow-livy-plugins" -> "Project interpreter", select
"Existing environment" and pick __python3__ executable from __venv__ folder
(__venv/bin/python3__)
* More IDEs to come! Send instructions for your favorite IDE in PRs.


TODO Formatting

todo debug


* (Optional) Debugging operators with `airflow test`.  In Pycharm, create a
new Python run configuration. Point it at __airflow-livy-plugins/airflow_home/venv/bin/airflow__
for "Script path", and in "Parameters" type in
`test TODO 2017-03-18T18:00:00.0`
where second and third words are the names of the DAG and the operator
you want to test, respectively. Plug the names of whatever DAG and operator
you'd like to test instead. Additionally, set up another Environment variable
in the same window to point at your __airflow__ directory. The result
should look like this:
```
PYTHONUNBUFFERED=1;AIRFLOW_HOME=/Users/vpanov/data/vpanov/airflow-livy-plugins/airflow_home
```

* Use PyCharm PyLint plugin to verify if your code conforms to PEP8 and other 
clean code guidelines. Just install it under "Preferences" -> "Plugins". If any 
questions arise, refer to [pylint-pycharm README.md on Github](https://github.com/leinardi/pylint-pycharm 
"pylint-pycharm GIT repo"). As soon as the plugin has been installed, set the
path to __.pylintrc__ as __airflow-livy-plugins/airflow_home/.pylintrc__ in
"Preferences" -> "PyLint" -> "Path to pylintrc".
Under "Arguments", type in `--load-plugins=pylint_airflow`. Don't forget to
exclude __venv__ folder (guidelines on the same README.md page
from Github) if you don't want to be bugged about code style errors in
Airflow and PySpark code.

http://michal.karzynski.pl/blog/2017/03/19/developing-workflows-with-apache-airflow/



https://github.com/panovvv/bigdata-docker-compose



```bash
python ./batches/join_2_files.py \
"file:////Users/vpanov/data/vpanov/bigdata-docker-compose/data/grades.csv" \
"file:///Users/vpanov/data/vpanov/bigdata-docker-compose/data/ssn-address.tsv" \
-file1_sep=, -file1_header=true \
-file1_schema="\`Last name\` STRING, \`First name\` STRING, SSN STRING, Test1 INT, Test2 INT, Test3 INT, Test4 INT, Final INT, Grade STRING" \
-file1_join_column=SSN -file2_header=false \
-file2_schema="\`Last name\` STRING, \`First name\` STRING, SSN STRING, Address1 STRING, Address2 STRING" \
-file2_join_column=SSN -output_header=true \
-output_columns="file1.\`Last name\` AS LastName, file1.\`First name\` AS FirstName, file1.SSN, file2.Address1, file2.Address2" 

# Optionally append to save result to file
#-output_path="file:///Users/vpanov/livy_batch_example" 
```


Spark Application Id: null when running livy in zeppelin means it's in local mode
appId null local mode

batch fail status is real in local mode.


YARN: 

Spark Application Id: application_1581953991557_0001

Spark WebUI: http://master:8088/proxy/application_1581953991557_0001/

Local:

Spark Application Id: null

Spark WebUI: null


linting: 

```bash
flake8
pylint --rcfile=.pylintrc --load-plugins=pylint_airflow airflow_home/
```


formatting:
black airflow_home/ batches/ tests/ && isort -rc airflow_home/ batches/ tests/

COverage
pytest --cov=airflow_home.plugins

tests
pytest

dev env and running the examples:
./airflow.sh up
./airflow.sh dev
./airflow.sh test - run tests with coverage

# See usage
./airflow.sh