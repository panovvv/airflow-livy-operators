from setuptools import setup

with open("README.md") as fh:
    long_description = fh.read()

setup(
    name="airflow-livy-operators-sexy",
    version="0.3.1",
    author="Sergei Sheremeta",
    author_email="s.w.sheremeta@gmail.com",
    description="Lets Airflow DAGs run Spark jobs via Livy: sessions and/or batches. Forked from panovvv/airflow-livy-operators and changed python to 3.5",
    #long_description=long_description,
    #long_description_content_type="text/markdown",
    url="https://github.com/ssheremeta/airflow-livy-operators",
    license="MIT License",
    packages=["airflow_livy"],
    package_dir={"airflow_livy": "airflow_home/plugins/airflow_livy"},
    python_requires=">=3.5",
)
