from setuptools import find_packages, setup

with open("README.md") as fh:
    long_description = fh.read()

setup(
    name="airflow-livy-plugins",
    version="0.1",
    author="Vadim Panov",
    author_email="headcra6@gmail.com",
    description="Plugins for Airflow to run Spark jobs via Livy: sessions and batches",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/panovvv/airflow-livy-plugins",
    license="MIT License",
    packages=find_packages(),
    python_requires=">=3.6",
)
