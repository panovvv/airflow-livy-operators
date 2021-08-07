# Airflow Livy Operators

[![Build Status](https://travis-ci.org/panovvv/airflow-livy-operators.svg?branch=master)](https://travis-ci.org/panovvv/airflow-livy-operators)
[![Code coverage](https://codecov.io/gh/panovvv/airflow-livy-operators/branch/master/graph/badge.svg)](https://codecov.io/gh/panovvv/airflow-livy-operators)

![PyPI](https://img.shields.io/pypi/v/airflow-livy-operators)
![Airflow dep version](https://img.shields.io/badge/airflow-2.1.2-green)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/airflow-livy-operators)

![PyPI - License](https://img.shields.io/pypi/l/airflow-livy-operators)


Lets Airflow DAGs run Spark jobs via Livy:
* Sessions,
* Batches. This mode supports additional verification via Spark/YARN REST API.

See [this blog post](https://shortn0tes.blogspot.com/2020/03/airflow-livy-spark.html "Blog post") for more information and detailed comparison of ways to run Spark jobs from Airflow.

## Directories and files of interest
* `airflow_home/plugins`: Airflow Livy operators' code.
* `airflow_home/dags`: example DAGs for Airflow.
* `batches`: Spark jobs code, to be used in Livy batches.
* `sessions`: Spark code for Livy sessions. You can add templates
to files' contents in order to pass parameters into it.
* `helper.sh`: helper shell script. Can be used to run sample DAGs,
prep development environment and more.
Run it to find out what other commands are available.


## How do I...

### ...run the examples?
Prerequisites:
* Python 3. Make sure it's installed and in __$PATH__
* Spark cluster with Livy. I heavily recommend you "mock" one on your machine with 
[my Spark cluster on Docker Compose](https://github.com/panovvv/bigdata-docker-compose).

Now, 
1. __Optional - this step can be skipped if you're mocking a cluster on your
machine__. Open *helper.sh*. Inside `init_airflow()` function you'll see Airflow
Connections for Livy, Spark and YARN. Redefine as appropriate.
1. Define the way the sample batch files from this repo are delivered to a cluster:
    1. if you're using a docker-compose cluster: redefine the BATCH_DIR variable 
    as appropriate. 
    1. if you're using your own cluster: modify the `copy_batches()` function so that it
    delivers the files to a place accessible by your cluster (could be `aws s3 cp` etc.)
1. run `./helper.sh up` to bring up the whole infrastructure. 
Airflow UI will be available at
[localhost:8888](http://localhost:8888 "Airflow UI").
   The credentials are `admin/admin`.
1. Ctrl+C to stop Airflow. Then `./helper.sh down` to dispose of
remaining Airflow processes (shouldn't be required if everything goes well.
Run this if you can't start Airflow again due to some non-informative errors) .

### ... use it in my project?
```bash
pip install airflow-livy-operators
```
This is how you import them:
```python
from airflow_livy.session import LivySessionOperator
from airflow_livy.batch import LivyBatchOperator
```
See sample DAGs under `airflow_home/dags` to learn how to use the operators.

### ... set up the development environment?
Alright, you want to contribute and need to be able to run the stuff on your machine,
as well as the usual niceness that comes with IDEs (debugging, syntax highlighting).

* `./helper.sh updev` runs Airflow with local operators' code (as opposed to 
pulling them from PyPi). Useful for development.
* `./helper.sh full` - run tests (pytest) with coverage report 
(will be saved to *htmlcov/*), highlight code style errors (flake8), 
reformat all code ([black](https://black.readthedocs.io/en/stable/) + 
[isort](https://readthedocs.org/projects/isort/))
* `./helper.sh ci` - same as above, but only check the code formatting. This
same command is ran by CI.
* (Pycharm-specific) point PyCharm to your newly-created virtual environment: go to
`"Preferences" -> "Project: airflow-livy-operators" -> "Project interpreter", select
"Existing environment"` and pick __python3__ executable from __venv__ folder
(__venv/bin/python3__)

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
* helper.sh - replace with modern tools (e.g. pipenv + Docker image)
* Disable some of flake8 flags for cleaner code