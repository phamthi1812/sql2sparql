#!/usr/bin/env python3
"""Debug calculated column in WHERE"""

from sql2sparql.parsers.sql_parser import SQLParser
from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from rdflib import Graph

# Test query
sql = "SELECT name, price, stock FROM product WHERE (price * stock) > 1000"

parser = SQLParser()
parsed = parser.parse(sql)

print(f"SQL Query: {sql}")
print()
print(f"Parsed result:")
print(f"  SELECT attributes: {parsed.select_attributes}")
print(f"  WHERE conditions: {parsed.where_conditions}")
print(f"  JOIN conditions: {parsed.join_conditions}")
print()

# Now try conversion
graph = Graph()
schema_mapper = SchemaMapper(graph)
converter = SQL2SPARQLConverter(schema_mapper)
sparql = converter.convert(sql)

print(f"Generated SPARQL:")
print(sparql)