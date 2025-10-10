#!/usr/bin/env python3
"""Simple test to debug the converter"""

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from rdflib import Graph
from pathlib import Path

# Load sample data
graph = Graph()
sample_data_path = Path("sql2sparql/examples/sample_data.ttl")
graph.parse(sample_data_path, format="turtle")

# Setup converter
schema_mapper = SchemaMapper(graph)
schema_mapper.extract_schema()
converter = SQL2SPARQLConverter(schema_mapper)

# Test simple calculated column
test_query = "SELECT name, price, stock, (price * stock) AS inventory_value FROM product WHERE (price * stock) > 1000"

print("SQL Query:")
print(test_query)

# Check what's being detected
select_clause = "name, price, stock, (price * stock) AS inventory_value"
items = converter._split_respecting_parens(select_clause, ',')
print("\nSplit items:", items)
for item in items:
    print(f"  Item: '{item}' - is_calc: {converter._is_calculated_expression(item)}")

print("\nConverted SPARQL:")
result = converter.convert(test_query)
print(result)

# Test the expression builder directly
from sql2sparql.core.converter import ExpressionBuilder
builder = ExpressionBuilder()

expr = "price * stock"
print("\n\nTesting expression parser for:", expr)
tree = builder.parse_expression(expr)
print("Parsed tree type:", tree.type, "value:", tree.value)
if tree.left:
    print("  Left:", tree.left.type, tree.left.value)
if tree.right:
    print("  Right:", tree.right.type, tree.right.value)

var_mappings = {"price": "?price", "stock": "?stock"}
sparql_expr = builder.to_sparql_expression(tree, var_mappings)
print("SPARQL expression:", sparql_expr)