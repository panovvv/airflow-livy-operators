from setuptools import setup

with open("README.md") as fh:
    long_description = fh.read()

setup(
    name="airflow-livy-plugins",
    version="0.2",
    author="Vadim Panov",
    author_email="headcra6@gmail.com",
    description="Plugins for Airflow to run Spark jobs via Livy: sessions and batches",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/panovvv/airflow-livy-plugins",
    license="MIT License",
    packages=["airflow_livy"],
    package_dir={"": "plugins"},
    python_requires=">=3.7",
)
