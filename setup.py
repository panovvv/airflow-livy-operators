from setuptools import setup

with open("README.md") as fh:
    long_description = fh.read()

setup(
    name="airflow-livy-operators",
    version="0.4.3",
    author="Vadim Panov",
    author_email="headcra6@gmail.com",
    description="Lets Airflow DAGs run Spark jobs via Livy: sessions and/or batches.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/panovvv/airflow-livy-operators",
    license="MIT License",
    packages=["airflow_livy"],
    package_dir={"airflow_livy": "airflow_home/plugins/airflow_livy"},
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
