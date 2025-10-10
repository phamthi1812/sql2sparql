#!/usr/bin/env python3
"""Debug FROM clause parsing"""

from sql2sparql.parsers.sql_parser import SQLParser
import sqlparse

# Test query with multiple tables
sql = """SELECT client.name, order.date
         FROM client, order
         WHERE client.subject = order.client"""

parser = SQLParser()

# First, let's see what tokens sqlparse generates
tokens = sqlparse.parse(sql)[0].flatten()
print("Tokens from sqlparse:")
for i, token in enumerate(tokens):
    if not token.is_whitespace:
        print(f"  {i}: '{token}' (type: {token.ttype})")

print("\n" + "="*50 + "\n")

# Parse and see what happens
print(f"SQL Query: {sql}")
print()

result = parser.parse(sql)

print(f"Parsed result:")
print(f"  FROM tables: {result.from_tables}")
print(f"  SELECT attributes: {result.select_attributes}")
print(f"  WHERE conditions: {result.where_conditions}")
print(f"  JOIN conditions: {result.join_conditions}")