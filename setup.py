from setuptools import setup

import pgexplode

with open("README.md", "r") as readme:
    long_description = readme.read()

setup(
    name="pgexplode",
    version=pgexplode.__version__,
    description="Utility for exploding a PostgreSQL table (and any related data) into separate schemas.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Dan Watson",
    author_email="dcwatson@gmail.com",
    url="https://github.com/dcwatson/pgexplode",
    project_urls={"Documentation": "https://dcwatson.github.io/pgexplode/"},
    license="MIT",
    py_modules=["pgexplode"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
)
