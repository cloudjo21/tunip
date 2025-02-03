from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="tunip",
    version="0.2.4",
    url="https://github.com/cloudjo21/tunip.git",
    packages=find_packages("src", exclude=["test*"]),
    package_dir={"tunip": "src/tunip"},
    python_requires=">=3.11.6",
    long_description=open("README.md").read(),
    install_requires=required,
    normalize_version=False,
)
