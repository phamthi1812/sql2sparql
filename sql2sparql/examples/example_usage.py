#!/usr/bin/env python3
"""
SQL2SPARQL Example Usage Script
Demonstrates various features of the SQL2SPARQL converter
"""

from pathlib import Path
from rdflib import Graph
from sql2sparql import SQL2SPARQLConverter, SchemaMapper, SPARQLExecutor
from sql2sparql.executors.sparql_executor import StoreType


def print_section(title):
    """Helper to print formatted section headers"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print('='*60)


def demo_basic_conversion():
    """Demonstrate basic SQL to SPARQL conversion"""
    print_section("Basic SQL to SPARQL Conversion")

    # Load sample RDF data
    data_file = Path(__file__).parent / "sample_data.ttl"
    graph = Graph()
    graph.parse(data_file, format="turtle")
    print(f"Loaded {len(graph)} triples from {data_file}")

    # Extract schema
    print("\nExtracting schema from RDF data...")
    schema_mapper = SchemaMapper(graph)
    schema = schema_mapper.extract_schema()

    # Print extracted schema
    print("\nExtracted Schema:")
    for table, attributes in schema.tables.items():
        print(f"  Table '{table}': {', '.join(attributes)}")

    # Create converter
    converter = SQL2SPARQLConverter(schema_mapper)

    # Example 1: Simple SELECT
    sql1 = "SELECT name, email FROM client"
    sparql1 = converter.convert(sql1)
    print(f"\nSQL: {sql1}")
    print(f"SPARQL:\n{sparql1}")

    # Execute and show results
    executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=graph)
    results = executor.execute_query(sparql1)
    print(f"\nResults ({len(results)} rows):")
    for row in results[:3]:
        print(f"  {row}")


def demo_complex_queries():
    """Demonstrate complex query conversions"""
    print_section("Complex Query Examples")

    # Load data and setup
    data_file = Path(__file__).parent / "sample_data.ttl"
    graph = Graph()
    graph.parse(data_file, format="turtle")

    schema_mapper = SchemaMapper(graph)
    schema_mapper.extract_schema()
    converter = SQL2SPARQLConverter(schema_mapper)
    executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=graph)

    # Example 1: Query with WHERE clause
    print("\n1. SELECT with WHERE condition:")
    sql = "SELECT name, price FROM product WHERE price > 100"
    sparql = converter.convert(sql)
    print(f"SQL: {sql}")
    results = executor.execute_query(sparql)
    print(f"Found {len(results)} products with price > 100")

    # Example 2: JOIN query
    print("\n2. JOIN query:")
    sql = """SELECT client.name, order.total
             FROM client, order
             WHERE client.subject = order.client_id"""
    sparql = converter.convert(sql)
    results = executor.execute_query(sparql)
    print(f"Found {len(results)} client-order pairs")

    # Example 3: Aggregate query
    print("\n3. Aggregate query:")
    sql = "SELECT category, COUNT(name) FROM product GROUP BY category"
    sparql = converter.convert(sql)
    print(f"SQL: {sql}")
    print(f"SPARQL:\n{sparql}")


def demo_insert_delete():
    """Demonstrate INSERT and DELETE operations"""
    print_section("INSERT and DELETE Operations")

    # Create a fresh graph for modifications
    graph = Graph()
    data_file = Path(__file__).parent / "sample_data.ttl"
    graph.parse(data_file, format="turtle")

    schema_mapper = SchemaMapper(graph)
    schema_mapper.extract_schema()
    converter = SQL2SPARQLConverter(schema_mapper)
    executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=graph)

    # Count clients before INSERT
    count_query = """
    SELECT (COUNT(?s) as ?count)
    WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/types/Client> }
    """
    result = executor.execute_query(count_query)
    initial_count = int(result[0]['count'])
    print(f"Initial client count: {initial_count}")

    # INSERT new client
    print("\nInserting new client...")
    insert_sql = """INSERT INTO client (name, email, age)
                    VALUES ('New User', 'new@example.com', 25)"""
    insert_sparql = converter.convert(insert_sql)
    print(f"SQL: {insert_sql}")
    print(f"SPARQL:\n{insert_sparql}")

    # Execute INSERT
    executor.execute_query(insert_sparql)

    # Count after INSERT
    result = executor.execute_query(count_query)
    new_count = int(result[0]['count'])
    print(f"Client count after INSERT: {new_count}")

    # DELETE example
    print("\nDeleting clients with age < 30...")
    delete_sql = "DELETE FROM client WHERE age < 30"
    delete_sparql = converter.convert(delete_sql)
    print(f"SQL: {delete_sql}")

    # Note: Actual deletion would be executed here
    # executor.execute_query(delete_sparql)


def demo_schema_validation():
    """Demonstrate schema validation features"""
    print_section("Schema Validation")

    # Load data and extract schema
    data_file = Path(__file__).parent / "sample_data.ttl"
    graph = Graph()
    graph.parse(data_file, format="turtle")

    schema_mapper = SchemaMapper(graph)
    schema_mapper.extract_schema()

    # Test valid references
    print("Testing schema references:")
    test_cases = [
        ("client", "name", True),
        ("client", "email", True),
        ("client", "invalid_column", False),
        ("invalid_table", "name", False),
        ("product", "price", True),
        ("product", "category", True),
    ]

    for table, attr, expected in test_cases:
        is_valid = schema_mapper.validate_sql_reference(table, attr)
        status = "✓" if is_valid == expected else "✗"
        print(f"  {status} {table}.{attr}: {is_valid}")


def demo_statistics():
    """Demonstrate RDF store statistics"""
    print_section("RDF Store Statistics")

    # Load data
    data_file = Path(__file__).parent / "sample_data.ttl"
    graph = Graph()
    graph.parse(data_file, format="turtle")

    # Get statistics
    executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=graph)
    stats = executor.get_statistics()

    print("Store Statistics:")
    for key, value in stats.items():
        print(f"  {key.replace('_', ' ').title()}: {value}")

    # Additional custom queries
    print("\nCustom Statistics:")

    # Count by type
    type_query = """
    SELECT ?type (COUNT(?s) as ?count)
    WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type }
    GROUP BY ?type
    ORDER BY DESC(?count)
    """
    results = executor.execute_query(type_query)
    print("\nEntities by type:")
    for row in results:
        type_name = row['type'].split('/')[-1]
        print(f"  {type_name}: {row['count']}")


def demo_error_handling():
    """Demonstrate error handling"""
    print_section("Error Handling")

    # Setup
    data_file = Path(__file__).parent / "sample_data.ttl"
    graph = Graph()
    graph.parse(data_file, format="turtle")

    schema_mapper = SchemaMapper(graph)
    schema_mapper.extract_schema()
    converter = SQL2SPARQLConverter(schema_mapper)

    # Test various error conditions
    test_queries = [
        ("Invalid SQL syntax", "SELECT FROM WHERE"),
        ("Unknown table", "SELECT * FROM unknown_table"),
        ("Invalid column", "SELECT invalid_column FROM client"),
    ]

    for description, sql in test_queries:
        print(f"\nTesting: {description}")
        print(f"SQL: {sql}")
        try:
            sparql = converter.convert(sql)
            print(f"Result: Converted successfully")
        except Exception as e:
            print(f"Error: {e.__class__.__name__}: {e}")


def main():
    """Run all demonstrations"""
    print("\n" + "="*60)
    print(" SQL2SPARQL Demonstration Script")
    print("="*60)

    demos = [
        ("Basic Conversion", demo_basic_conversion),
        ("Complex Queries", demo_complex_queries),
        ("INSERT/DELETE Operations", demo_insert_delete),
        ("Schema Validation", demo_schema_validation),
        ("Statistics", demo_statistics),
        ("Error Handling", demo_error_handling),
    ]

    print("\nAvailable demonstrations:")
    for i, (name, _) in enumerate(demos, 1):
        print(f"  {i}. {name}")

    # Run all demos
    for name, demo_func in demos:
        try:
            demo_func()
        except Exception as e:
            print(f"\nError in {name}: {e}")

    print("\n" + "="*60)
    print(" Demonstration Complete")
    print("="*60)


if __name__ == "__main__":
    main()
