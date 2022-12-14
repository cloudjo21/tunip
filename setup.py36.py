from setuptools import setup, find_packages
import setuptools

with open("requirements.py36.txt") as f:
    required = f.read().splitlines()

setup(
    name="tunip",
    version="0.0.13",
    url="https://github.com/cloudjo21/tunip.git",
    packages=find_packages("src"),
    package_dir={"tunip": "src/tunip"},
    python_requires=">=3.6",
    long_description=open("README.md").read(),
    install_requires=required,
)
