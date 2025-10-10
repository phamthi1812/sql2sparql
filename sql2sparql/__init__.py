"""
SQL2SPARQL - Automatic SQL to SPARQL Conversion for Direct RDF Querying

This package provides a comprehensive framework for converting SQL queries to SPARQL queries,
enabling SQL users to query RDF data without conversion to relational format.
"""

__version__ = "1.0.0"
__author__ = "SQL2SPARQL Team"

from .core.converter import SQL2SPARQLConverter
from .core.schema_mapper import SchemaMapper
from .executors.sparql_executor import SPARQLExecutor
from .core.models import SQLQuery, SPARQLQuery, Triple, Attribute

__all__ = [
    "SQL2SPARQLConverter",
    "SchemaMapper", 
    "SPARQLExecutor",
    "SQLQuery",
    "SPARQLQuery",
    "Triple",
    "Attribute",
]
