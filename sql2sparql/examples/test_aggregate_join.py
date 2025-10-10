#!/usr/bin/env python3
"""Debug aggregate with JOIN issue"""

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from sql2sparql.parsers.sql_parser import SQLParser
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

# Test aggregate with JOIN
test_query = """SELECT client.name, SUM(order.total) as total_spent
                FROM client, order
                WHERE client.id = order.client_id
                GROUP BY client.name"""

print("SQL Query:")
print(test_query)

# First test the parser
parser = SQLParser()
parsed = parser.parse(test_query)

print("\nDebug parsing:")
import sqlparse
parsed_sql = sqlparse.parse(test_query)[0]
tokens = list(parsed_sql.flatten())
print(f"  Total tokens: {len(tokens)}")
for i, token in enumerate(tokens[:30]):
    if not token.is_whitespace:
        print(f"    {i}: '{token}' (type: {token.ttype})")

print("\nParsed Query:")
print(f"  Select attributes: {[(a.relation, a.name, a.aggregate) for a in parsed.select_attributes]}")
print(f"  From tables: {parsed.from_tables}")
print(f"  Join conditions: {[(j.left_operand.relation, j.left_operand.name, j.right_operand.relation, j.right_operand.name) for j in parsed.join_conditions]}")
print(f"  Group by: {[(a.relation, a.name) for a in parsed.group_by]}")

print("\nConverted SPARQL:")
result = converter.convert(test_query)
print(result)