#!/usr/bin/env python3
"""
Quick demonstration of SQL2SPARQL functionality
"""

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from sql2sparql.parsers.sql_parser import SQLParser
from rdflib import Graph, Namespace, Literal, RDF

# Create sample RDF data
graph = Graph()
ex = Namespace("http://example.org/")
ont = Namespace("http://example.org/ontology/")
types = Namespace("http://example.org/types/")

# Add sample client data
graph.add((ex.client1, RDF.type, types.Client))
graph.add((ex.client1, ont.name, Literal("John Doe")))
graph.add((ex.client1, ont.email, Literal("john@example.com")))
graph.add((ex.client1, ont.age, Literal(35)))

graph.add((ex.client2, RDF.type, types.Client))
graph.add((ex.client2, ont.name, Literal("Jane Smith")))
graph.add((ex.client2, ont.email, Literal("jane@example.com")))
graph.add((ex.client2, ont.age, Literal(28)))

# Extract schema
print("Extracting schema from RDF data...")
schema_mapper = SchemaMapper(graph)
schema = schema_mapper.extract_schema()

print("\nExtracted Tables:")
for table, attrs in schema.tables.items():
    print(f"  {table}: {attrs}")

# Create converter
converter = SQL2SPARQLConverter(schema_mapper)

# Test conversions
test_queries = [
    "SELECT name, email FROM client",
    "SELECT name FROM client WHERE age > 30",
    "INSERT INTO client (name, email) VALUES ('Bob', 'bob@example.com')",
    "DELETE FROM client WHERE age < 25"
]

print("\n" + "="*60)
print("SQL to SPARQL Conversion Examples")
print("="*60)

for sql in test_queries:
    print(f"\nSQL: {sql}")
    print("-" * 40)
    try:
        sparql = converter.convert(sql)
        print(f"SPARQL:\n{sparql}")
    except Exception as e:
        print(f"Error: {e}")

print("\n" + "="*60)
print("Demo Complete!")
print("="*60)