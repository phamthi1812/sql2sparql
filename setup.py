"""
Setup script for SQL2SPARQL
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sql2sparql",
    version="1.0.0",
    author="SQL2SPARQL Team",
    author_email="contact@sql2sparql.org",
    description="Automatic SQL to SPARQL converter for direct RDF querying",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/sql2sparql",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "rdflib>=6.0.0",
        "SPARQLWrapper>=2.0.0",
        "sqlparse>=0.4.0",
        "click>=8.0.0",
        "pydantic>=2.0.0",
        "requests>=2.28.0",
        "rich>=13.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
            "pre-commit>=3.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "sql2sparql=sql2sparql.cli.main:cli",
        ],
    },
    package_data={
        "sql2sparql": [
            "examples/*.ttl",
            "examples/*.sql",
            "examples/*.py",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)