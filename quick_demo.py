#!/usr/bin/env python3
"""
SQL2SPARQL Comprehensive Demo

This script demonstrates the conversion of complex SQL queries to SPARQL queries
using the SQL2SPARQL tool. It includes a variety of SQL constructs and demonstrates
the full conversion process with expected results.

The test queries cover:
- Simple SELECT with WHERE conditions
- Complex JOINs across multiple tables
- Aggregate functions (COUNT, SUM, AVG, MAX, MIN)
- GROUP BY and HAVING clauses
- ORDER BY with LIMIT
- Pattern matching (LIKE)
- Date/time comparisons
- UNION operations
- INSERT and DELETE operations
- Subqueries and advanced filtering

Run this script to see comprehensive SQL to SPARQL conversion examples.
"""

import json
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from sql2sparql.executors.sparql_executor import SPARQLExecutor, StoreType
from rdflib import Graph

console = Console()

class SQL2SPARQLDemo:
    """Comprehensive demo of SQL2SPARQL conversion capabilities"""

    def __init__(self):
        """Initialize the demo with sample data and converter"""
        self.setup_data()
        self.setup_converter()

    def setup_data(self):
        """Load sample RDF data"""
        console.print("[blue]Loading sample RDF data...[/blue]")

        # Load the sample data
        self.graph = Graph()
        sample_data_path = Path(__file__).parent / "sql2sparql" / "examples" / "sample_data.ttl"

        if not sample_data_path.exists():
            console.print(f"[red]Sample data not found at: {sample_data_path}[/red]")
            sys.exit(1)

        self.graph.parse(sample_data_path, format="turtle")
        console.print(f"[green]✓ Loaded {len(self.graph)} triples[/green]")

    def setup_converter(self):
        """Setup the SQL2SPARQL converter with schema mapping"""
        console.print("[blue]Setting up converter with schema extraction...[/blue]")

        # Extract schema from RDF data
        self.schema_mapper = SchemaMapper(self.graph)
        schema = self.schema_mapper.extract_schema()

        # Display extracted schema
        table = Table(title="Extracted Schema")
        table.add_column("Table", style="cyan")
        table.add_column("Attributes", style="green")

        for table_name, attributes in schema.tables.items():
            table.add_row(table_name, ", ".join(attributes))

        console.print(table)

        # Create converter and executor
        self.converter = SQL2SPARQLConverter(self.schema_mapper)
        self.executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=self.graph)
        console.print("[green]✓ Converter ready[/green]")

    def get_complex_test_queries(self):
        """
        Define comprehensive set of complex SQL test queries
        Each query tests different SQL constructs and conversion capabilities
        """
        return [
            {
                "title": "1. Simple SELECT with WHERE condition",
                "description": "Basic SELECT with numeric comparison",
                "sql": "SELECT name, price FROM product WHERE price > 100",
                "expected_features": ["SELECT projection", "WHERE filtering", "Numeric comparison"]
            },

            {
                "title": "2. Multiple WHERE conditions with AND/OR",
                "description": "Complex WHERE clause with multiple conditions",
                "sql": "SELECT name, category, price FROM product WHERE category = 'Electronics' AND price < 500 OR stock > 20",
                "expected_features": ["Multiple WHERE conditions", "AND/OR operators", "Mixed data types"]
            },

            {
                "title": "3. Pattern matching with LIKE",
                "description": "String pattern matching in WHERE clause",
                "sql": "SELECT name, email FROM client WHERE email LIKE '%example.com'",
                "expected_features": ["Pattern matching", "LIKE operator", "String filtering"]
            },

            {
                "title": "4. JOIN across two tables",
                "description": "Inner join between clients and orders",
                "sql": "SELECT client.name, order.date, order.total FROM client, order WHERE client.id = order.client_id",
                "expected_features": ["JOIN operation", "Multiple table access", "Relationship navigation"]
            },

            {
                "title": "5. Complex three-way JOIN",
                "description": "Join across clients, orders, and order items",
                "sql": """SELECT client.name, product.name, orderitem.quantity
                         FROM client, order, orderitem, product
                         WHERE client.id = order.client_id
                           AND order.id = orderitem.order
                           AND orderitem.product = product.id""",
                "expected_features": ["Multi-table JOIN", "Complex relationships", "Chain joins"]
            },

            {
                "title": "6. Aggregate function - COUNT with GROUP BY",
                "description": "Counting products by category",
                "sql": "SELECT category, COUNT(name) FROM product GROUP BY category",
                "expected_features": ["COUNT aggregate", "GROUP BY clause", "Aggregation"]
            },

            {
                "title": "7. Multiple aggregates with HAVING",
                "description": "Complex aggregation with filtering",
                "sql": "SELECT category, COUNT(name), AVG(price), SUM(stock) FROM product GROUP BY category HAVING COUNT(name) > 1",
                "expected_features": ["Multiple aggregates", "HAVING clause", "Post-aggregation filtering"]
            },

            {
                "title": "8. ORDER BY with LIMIT",
                "description": "Sorted results with pagination",
                "sql": "SELECT name, price FROM product ORDER BY price DESC LIMIT 3",
                "expected_features": ["ORDER BY sorting", "DESC ordering", "LIMIT clause"]
            },

            {
                "title": "9. Complex aggregate with JOIN and ORDER BY",
                "description": "Total orders per client, sorted by total",
                "sql": """SELECT client.name, SUM(order.total) as total_spent
                         FROM client, order
                         WHERE client.id = order.client_id
                         GROUP BY client.name
                         ORDER BY total_spent DESC""",
                "expected_features": ["Aggregate with JOIN", "Alias in SELECT", "Complex sorting"]
            },

            {
                "title": "10. DISTINCT with JOIN",
                "description": "Unique clients with pending orders",
                "sql": """SELECT DISTINCT client.name, client.email
                         FROM client, order
                         WHERE client.id = order.client_id
                           AND order.status = 'pending'""",
                "expected_features": ["DISTINCT modifier", "JOIN with filter", "Unique results"]
            },

            {
                "title": "11. Date comparison and filtering",
                "description": "Recent orders with date filtering",
                "sql": """SELECT client.name, order.date, order.total
                         FROM client, order
                         WHERE client.id = order.client_id
                           AND order.date > '2024-01-01'
                         ORDER BY order.date DESC""",
                "expected_features": ["Date comparison", "Temporal filtering", "Date sorting"]
            },

            {
                "title": "12. Calculated columns",
                "description": "Products with inventory value calculation",
                "sql": "SELECT name, price, stock, (price * stock) AS inventory_value FROM product WHERE (price * stock) > 1000",
                "expected_features": ["Calculated columns", "Arithmetic expressions", "Column aliases"]
            },

            {
                "title": "13. UNION operation",
                "description": "Combine results from multiple queries",
                "sql": """SELECT name FROM client
                         UNION
                         SELECT name FROM product
                         UNION
                         SELECT name FROM supplier""",
                "expected_features": ["UNION operator", "Result combination", "Multiple sources"]
            },

            {
                "title": "14. Supplier filtering by country",
                "description": "Geographic filtering with exact match",
                "sql": "SELECT name, contact FROM supplier WHERE country = 'USA'",
                "expected_features": ["Exact string matching", "Geographic filtering", "Simple WHERE"]
            },

            {
                "title": "15. Complex filtering with status and stock",
                "description": "Multi-condition product filtering",
                "sql": """SELECT name, stock, price
                         FROM product
                         WHERE stock < 20
                           AND category IN ('Electronics', 'Furniture')
                         ORDER BY stock ASC""",
                "expected_features": ["IN operator", "Multiple conditions", "Ascending sort"]
            },

            {
                "title": "16. INSERT operation",
                "description": "Add new client record",
                "sql": "INSERT INTO client (name, email, age, status) VALUES ('Alice Brown', 'alice.brown@example.com', 30, 'active')",
                "expected_features": ["INSERT statement", "Value insertion", "Multiple columns"]
            },

            {
                "title": "17. DELETE operation with WHERE",
                "description": "Remove inactive clients",
                "sql": "DELETE FROM client WHERE status = 'inactive'",
                "expected_features": ["DELETE statement", "Conditional deletion", "Data modification"]
            },

            {
                "title": "18. Range query with BETWEEN",
                "description": "Products in specific price range",
                "sql": "SELECT name, price FROM product WHERE price BETWEEN 50 AND 300",
                "expected_features": ["BETWEEN operator", "Range filtering", "Numeric ranges"]
            },

            {
                "title": "19. MAX and MIN aggregates",
                "description": "Find price extremes by category",
                "sql": "SELECT category, MAX(price) as max_price, MIN(price) as min_price FROM product GROUP BY category",
                "expected_features": ["MAX/MIN functions", "Multiple aggregates", "Column aliases"]
            },

            {
                "title": "20. Complex nested condition",
                "description": "Orders with complex business logic",
                "sql": """SELECT client.name, order.date, order.total, order.status
                         FROM client, order
                         WHERE client.id = order.client_id
                           AND ((order.status = 'completed' AND order.total > 500)
                                OR (order.status = 'pending' AND order.date > '2024-01-01'))""",
                "expected_features": ["Nested conditions", "Parentheses grouping", "Complex business logic"]
            }
        ]

    def run_conversion_test(self, query_info):
        """
        Run a single SQL to SPARQL conversion test

        Args:
            query_info: Dictionary containing query information

        Returns:
            Dictionary with test results
        """
        console.print(f"\n[bold cyan]{query_info['title']}[/bold cyan]")
        console.print(f"[dim]{query_info['description']}[/dim]")
        console.print(f"[yellow]Features tested: {', '.join(query_info['expected_features'])}[/yellow]")

        result = {
            "title": query_info["title"],
            "sql": query_info["sql"],
            "success": False,
            "sparql": None,
            "error": None,
            "execution_result": None
        }

        try:
            # Convert SQL to SPARQL
            console.print("\n[blue]Converting SQL to SPARQL...[/blue]")
            sparql_query = self.converter.convert(query_info["sql"])
            result["sparql"] = sparql_query
            result["success"] = True

            # Display SQL query
            console.print("\n[bold]SQL Query:[/bold]")
            sql_syntax = Syntax(query_info["sql"], "sql", theme="monokai", line_numbers=True)
            console.print(Panel(sql_syntax))

            # Display SPARQL query
            console.print("\n[bold]Generated SPARQL:[/bold]")
            sparql_syntax = Syntax(sparql_query, "sparql", theme="monokai", line_numbers=True)
            console.print(Panel(sparql_syntax))

            # Try to execute the query (if it's a SELECT query)
            if query_info["sql"].strip().upper().startswith("SELECT"):
                try:
                    console.print("\n[yellow]Executing SPARQL query...[/yellow]")
                    execution_results = self.executor.execute_query(sparql_query)

                    if isinstance(execution_results, list) and execution_results:
                        result["execution_result"] = execution_results

                        # Display results in a table
                        table = Table(title="Query Results")

                        # Add columns dynamically based on first result
                        for key in execution_results[0].keys():
                            table.add_column(key, style="green")

                        # Add up to 10 rows for display
                        for row in execution_results[:10]:
                            table.add_row(*[str(row.get(k, '')) for k in execution_results[0].keys()])

                        console.print(table)

                        if len(execution_results) > 10:
                            console.print(f"[dim]... and {len(execution_results) - 10} more rows[/dim]")

                        console.print(f"[green]✓ Query executed successfully - {len(execution_results)} results[/green]")
                    else:
                        console.print("[yellow]Query executed but returned no results[/yellow]")
                        result["execution_result"] = []

                except Exception as e:
                    console.print(f"[yellow]Query conversion succeeded but execution failed: {e}[/yellow]")
                    result["execution_result"] = f"Execution error: {e}"
            else:
                console.print("[blue]Non-SELECT query - skipping execution[/blue]")
                result["execution_result"] = "Non-SELECT query"

            console.print("[green]✓ Conversion successful[/green]")

        except Exception as e:
            console.print(f"[red]✗ Conversion failed: {e}[/red]")
            result["error"] = str(e)
            result["success"] = False

        return result

    def run_all_tests(self):
        """Run all conversion tests and collect results"""
        console.print("\n[bold magenta]Starting SQL2SPARQL Comprehensive Demo[/bold magenta]")
        console.print("[dim]Testing complex SQL queries and their SPARQL conversions[/dim]")

        test_queries = self.get_complex_test_queries()
        results = []

        for i, query_info in enumerate(test_queries, 1):
            console.print(f"\n{'='*80}")
            console.print(f"[bold]Test {i}/{len(test_queries)}[/bold]")

            result = self.run_conversion_test(query_info)
            results.append(result)

            # Small pause for readability
            import time
            time.sleep(0.5)

        return results

    def generate_summary_report(self, results):
        """Generate a comprehensive summary report"""
        console.print(f"\n{'='*80}")
        console.print("[bold magenta]COMPREHENSIVE TEST SUMMARY REPORT[/bold magenta]")
        console.print(f"{'='*80}")

        # Statistics
        total_tests = len(results)
        successful_conversions = len([r for r in results if r["success"]])
        successful_executions = len([r for r in results if r.get("execution_result") and
                                   isinstance(r["execution_result"], list)])

        stats_table = Table(title="Test Statistics")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="yellow", justify="right")

        stats_table.add_row("Total Tests", str(total_tests))
        stats_table.add_row("Successful Conversions", f"{successful_conversions}/{total_tests}")
        stats_table.add_row("Successful Executions", f"{successful_executions}/{total_tests}")
        stats_table.add_row("Conversion Success Rate", f"{successful_conversions/total_tests*100:.1f}%")

        console.print(stats_table)

        # Detailed results table
        console.print("\n[bold]Detailed Results:[/bold]")
        results_table = Table()
        results_table.add_column("Test", style="cyan", width=40)
        results_table.add_column("Conversion", style="green", justify="center")
        results_table.add_column("Execution", style="blue", justify="center")
        results_table.add_column("Results", style="yellow", justify="center")

        for result in results:
            conversion_status = "✓" if result["success"] else "✗"

            execution_status = "N/A"
            result_count = "N/A"

            if result["success"] and result.get("execution_result") is not None:
                if isinstance(result["execution_result"], list):
                    execution_status = "✓"
                    result_count = str(len(result["execution_result"]))
                elif result["execution_result"] == "Non-SELECT query":
                    execution_status = "N/A"
                    result_count = "N/A"
                else:
                    execution_status = "✗"
                    result_count = "Error"

            results_table.add_row(
                result["title"][:37] + "..." if len(result["title"]) > 40 else result["title"],
                conversion_status,
                execution_status,
                result_count
            )

        console.print(results_table)

        # Show any errors
        errors = [r for r in results if not r["success"]]
        if errors:
            console.print("\n[bold red]Conversion Errors:[/bold red]")
            for error in errors:
                console.print(f"• {error['title']}: {error['error']}")

def main():
    """Main demo function with comprehensive testing"""
    try:
        # Initialize and run demo
        demo = SQL2SPARQLDemo()

        # Run all tests
        console.print("[bold blue]Running comprehensive SQL2SPARQL conversion tests...[/bold blue]")
        results = demo.run_all_tests()

        # Generate report
        demo.generate_summary_report(results)

        # Save detailed results to file
        output_file = "sql2sparql_test_results.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)

        console.print(f"\n[green]✓ Detailed results saved to {output_file}[/green]")
        console.print("\n[bold cyan]Demo completed successfully![/bold cyan]")

        return results

    except Exception as e:
        console.print(f"[red]Demo failed: {e}[/red]")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Expected Results Comment Block:
    """
    EXPECTED RESULTS FOR SQL2SPARQL CONVERSION DEMO:

    This demo tests 20 complex SQL queries covering various SQL constructs and their
    conversion to SPARQL. The expected results include:

    1. BASIC QUERIES (1-3): Simple SELECT with WHERE conditions
       - Should convert to basic SPARQL SELECT with FILTER clauses
       - Pattern matching (LIKE) should convert to SPARQL regex filters

    2. JOIN QUERIES (4-5): Table relationships and joins
       - Should generate appropriate triple patterns linking entities
       - Multi-table joins should create proper variable bindings

    3. AGGREGATE QUERIES (6-9): COUNT, SUM, AVG, etc.
       - Should convert to SPARQL aggregate functions
       - GROUP BY should generate proper grouping expressions
       - HAVING should convert to post-aggregation filters

    4. SORTING AND LIMITS (8, 11): ORDER BY and LIMIT clauses
       - Should generate SPARQL ORDER BY with ASC/DESC
       - LIMIT should convert directly to SPARQL LIMIT

    5. ADVANCED FEATURES (10-15): DISTINCT, UNION, calculated columns
       - DISTINCT should convert to SPARQL DISTINCT
       - UNION should generate SPARQL UNION blocks
       - Calculated expressions should use SPARQL expressions

    6. DATA MODIFICATION (16-17): INSERT and DELETE operations
       - INSERT should convert to SPARQL INSERT DATA
       - DELETE should convert to SPARQL DELETE WHERE

    7. COMPLEX CONDITIONS (18-20): BETWEEN, nested conditions
       - Complex logical expressions should be properly converted
       - BETWEEN should convert to appropriate range filters

    SUCCESS EXPECTATIONS:
    - Conversion Success Rate: 85-95% (some advanced features may not be fully implemented)
    - Execution Success Rate: 70-85% (depends on schema mapping accuracy)
    - All basic SELECT queries should convert and execute successfully
    - JOIN queries should generate proper relationship patterns
    - Aggregate queries should work for simple cases
    - Complex queries may have partial success depending on implementation completeness

    The tool demonstrates the research paper's algorithms in practice, showing how
    SQL relational queries can be systematically converted to SPARQL graph queries
    while preserving query semantics and enabling direct RDF querying.
    """

    main()