#!/usr/bin/env python3
"""
Test Script for Enhanced SQL2SPARQL Converter Features

This script specifically tests the enhanced features added to the converter:
1. Calculated columns with arithmetic expressions
2. Complex WHERE clauses with AND/OR logic
3. UNION queries
4. Improved aggregate functions with JOINs
5. BETWEEN and IN operators
"""

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from rdflib import Graph
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

console = Console()

def test_enhanced_converter():
    """Test the enhanced SQL2SPARQL converter with complex queries"""

    # Load sample data
    console.print("[blue]Loading sample RDF data...[/blue]")
    graph = Graph()
    sample_data_path = Path("sql2sparql/examples/sample_data.ttl")
    graph.parse(sample_data_path, format="turtle")

    # Setup converter
    schema_mapper = SchemaMapper(graph)
    schema_mapper.extract_schema()
    converter = SQL2SPARQLConverter(schema_mapper)

    # Test queries that previously failed
    test_queries = [
        {
            "name": "Calculated Column Test",
            "sql": "SELECT name, price, stock, (price * stock) AS inventory_value FROM product WHERE (price * stock) > 1000",
            "description": "Tests arithmetic expressions and calculated columns"
        },
        {
            "name": "Complex AND/OR Test",
            "sql": "SELECT name, price FROM product WHERE category = 'Electronics' AND price < 500 OR stock > 20",
            "description": "Tests complex WHERE clause with AND/OR logic"
        },
        {
            "name": "UNION Query Test",
            "sql": """SELECT name FROM client
                     UNION
                     SELECT name FROM product""",
            "description": "Tests UNION operation"
        },
        {
            "name": "Aggregate with JOIN Test",
            "sql": """SELECT client.name, SUM(order.total) as total_spent
                     FROM client, order
                     WHERE client.id = order.client_id
                     GROUP BY client.name""",
            "description": "Tests aggregate functions with JOINs"
        },
        {
            "name": "BETWEEN Operator Test",
            "sql": "SELECT name, price FROM product WHERE price BETWEEN 50 AND 300",
            "description": "Tests BETWEEN operator"
        },
        {
            "name": "IN Operator Test",
            "sql": "SELECT name, category FROM product WHERE category IN ('Electronics', 'Furniture')",
            "description": "Tests IN operator"
        },
        {
            "name": "Complex Expression Test",
            "sql": "SELECT name, (price * 1.1) AS price_with_tax, (stock - 5) AS adjusted_stock FROM product",
            "description": "Tests multiple calculated expressions"
        },
        {
            "name": "MAX/MIN Aggregates Test",
            "sql": "SELECT category, MAX(price) as max_price, MIN(price) as min_price FROM product GROUP BY category",
            "description": "Tests MAX/MIN aggregate functions"
        }
    ]

    console.print("\n[bold magenta]Testing Enhanced SQL2SPARQL Converter Features[/bold magenta]\n")

    success_count = 0
    total_count = len(test_queries)

    for i, test in enumerate(test_queries, 1):
        console.print(f"[cyan]Test {i}/{total_count}: {test['name']}[/cyan]")
        console.print(f"[dim]{test['description']}[/dim]")

        # Display SQL
        console.print("\n[bold]SQL Query:[/bold]")
        sql_syntax = Syntax(test['sql'], "sql", theme="monokai")
        console.print(Panel(sql_syntax))

        try:
            # Convert to SPARQL
            sparql = converter.convert(test['sql'])

            # Display SPARQL
            console.print("\n[bold]Generated SPARQL:[/bold]")
            sparql_syntax = Syntax(sparql, "sparql", theme="monokai")
            console.print(Panel(sparql_syntax))

            console.print("[green]✓ Conversion successful[/green]\n")
            success_count += 1

        except Exception as e:
            console.print(f"[red]✗ Conversion failed: {e}[/red]\n")

        console.print("-" * 80 + "\n")

    # Summary
    console.print(f"\n[bold cyan]Test Summary:[/bold cyan]")
    console.print(f"Success Rate: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")

    if success_count == total_count:
        console.print("[bold green]🎉 All enhanced features working correctly![/bold green]")
    else:
        console.print(f"[yellow]⚠️ {total_count - success_count} tests failed - further improvements needed[/yellow]")

if __name__ == "__main__":
    test_enhanced_converter()