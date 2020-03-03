# Airflow Livy Plugins

[![Build Status](https://travis-ci.org/panovvv/airflow-livy-plugins.svg?branch=master)](https://travis-ci.org/panovvv/airflow-livy-plugins)
[![Code coverage](https://codecov.io/gh/panovvv/airflow-livy-plugins/branch/master/graph/badge.svg)](https://codecov.io/gh/panovvv/airflow-livy-plugins)

Plugins for Airflow to run Spark jobs via Livy: 
* Sessions,
* Batches. This mode supports additional verification via Spark/YARN REST API.

See [this blog post](https://www.shortn0tes.com/2019/08/airflow-spark-livy-sessions-batches.html "Blog post") for more information and detailed comparison of ways to run Spark jobs from Airflow.

## Directories and files of interest
* `airflow_home`: example DAGs and plugins for Airflow. Can be used as 
Airflow home path.
* `batches`: Spark jobs code, to be used in Livy batches.
* `sessions`: (Optionally) templated Spark code for Livy sessions.
* `airflow.sh`: helper shell script. Can be used to run sample DAGs,
prep development environment and more.
Run it to find out what other commands are available.


## How do I...

### ...run the examples?
Prerequisites:
* Python 3. Make sure it's installed and in __$PATH__

Now, 
1. Do you have a Spark cluster with Livy running somewhere?
    1. *No*. Either get one, or "mock" it with 
    [my Spark cluster on Docker Compose](https://github.com/panovvv/bigdata-docker-compose).
    1. *Yes*. You're golden!
1. __Optional - this step can be skipped if you're mocking a cluster on your
machine__. Open *airflow.sh*. Inside `init_airflow ()` function you'll see Airflow
Connections for Livy, Spark and YARN. Redefine as appropriate.
1. run `./airflow.sh up` to bring up the whole infrastructure. 
Airflow UI will be available at
[localhost:8080](http://localhost:8888 "Airflow UI").
1. Ctrl+C to stop Airflow. Then `./airflow.sh down` to dispose of
remaining Airflow processes (shouldn't be needed there if everything goes well).

### ... use it in my project?
```bash
pip install airflow-livy-plugins
```
Then link or copy the plugin files into `$AIRFLOW_HOME/plugins` 
(see how I do that in `./airflow.sh`). 
They'll get loaded into Airflow via Plugin Manager automatically.
This is how you import the plugins:
```python
from airflow.operators import LivySessionOperator
from airflow.operators import LivyBatchOperator
```
Plugins are loaded at run-time so the imports above will look broken in your IDE,
but will work fine in Airflow.
Take a look at the sample DAGs to see my walkaround :)

### ... set up the development environment?
Alright, you want to contribute and need to be able to run the stuff on your machine,
as well as the usual niceness that comes with IDEs (debugging, syntax highlighting). How do I

* run `./airflow.sh dev` to install all dev dependencies.
* `./airflow.sh updev` runs local Airflow with local plugins (as opposed to 
pulling them from PyPi)
* (Pycharm-specific) point PyCharm to your newly-created virtual environment: go to
`"Preferences" -> "Project: airflow-livy-plugins" -> "Project interpreter", select
"Existing environment"` and pick __python3__ executable from __venv__ folder
(__venv/bin/python3__)
* `./airflow.sh cov` - run tests with coverage report 
(will be saved to *htmlcov/*).
* `./airflow.sh lint` - highlight code style errors.
* `./airflow.sh format` to reformat all code 
([Black](https://black.readthedocs.io/en/stable/) + 
[isort](https://readthedocs.org/projects/isort/))

### ... debug?

* (Pycharm-specific) Step-by-step debugging with `airflow test` 
and running PySpark batch jobs locally (with debugging as well) 
is supported via run configurations under `.idea/runConfigurations`.
You shouldn't have to do anything to use them - just open the folder
in PyCharm as a project.
* An example of how a batch can be ran on local Spark:
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

## TODO
* airflow.sh - replace with modern tools (e.g. pipenv + Docker image)
* Disable some of flake8 flags for cleaner code