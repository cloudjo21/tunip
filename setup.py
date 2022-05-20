from setuptools import setup, find_packages
import setuptools

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="tunip",
    version="0.0.13",
    url="https://bitbucket.org/peterleecodiit/tunip.git",
    packages=find_packages("src"),
    package_dir={"tunip": "src/tunip"},
    python_requires=">=3.8",
    long_description=open("README.md").read(),
    install_requires=required,
)
