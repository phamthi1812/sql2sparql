"""
SQL2SPARQL Command Line Interface
"""
import click
import json
import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.syntax import Syntax
from rich.panel import Panel
from rdflib import Graph

from ..core.converter import SQL2SPARQLConverter
from ..core.schema_mapper import SchemaMapper
from ..executors.sparql_executor import SPARQLExecutor, StoreType
from ..parsers.sql_parser import SQLParser

console = Console()


@click.group()
@click.version_option(version="1.0.0", prog_name="sql2sparql")
def cli():
    """SQL2SPARQL - Convert SQL queries to SPARQL for RDF querying"""
    pass


@cli.command()
@click.option('--sql', '-s', required=True, help='SQL query to convert')
@click.option('--rdf-file', '-r', help='RDF file for schema extraction')
@click.option('--format', '-f', default='turtle', help='RDF file format')
@click.option('--output', '-o', help='Output file for SPARQL query')
@click.option('--execute', '-e', is_flag=True, help='Execute the generated SPARQL query')
@click.option('--pretty', '-p', is_flag=True, default=True, help='Pretty print output')
def convert(sql, rdf_file, format, output, execute, pretty):
    """Convert SQL query to SPARQL"""
    try:
        # Initialize components
        schema_mapper = None
        executor = None

        # Load RDF data if provided
        if rdf_file:
            console.print(f"[blue]Loading RDF file: {rdf_file}[/blue]")
            graph = Graph()
            graph.parse(rdf_file, format=format)

            # Extract schema
            schema_mapper = SchemaMapper(graph)
            schema = schema_mapper.extract_schema()

            # Create executor
            executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=graph)

            # Show extracted schema
            if pretty:
                table = Table(title="Extracted Schema")
                table.add_column("Table", style="cyan")
                table.add_column("Attributes", style="green")

                for table_name, attributes in schema.tables.items():
                    table.add_row(table_name, ", ".join(attributes))

                console.print(table)

        # Create converter
        converter = SQL2SPARQLConverter(schema_mapper)

        # Convert SQL to SPARQL
        console.print("\n[yellow]Converting SQL to SPARQL...[/yellow]")
        sparql_query = converter.convert(sql)

        # Display results
        if pretty:
            console.print("\n[bold]SQL Query:[/bold]")
            syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
            console.print(Panel(syntax))

            console.print("\n[bold]SPARQL Query:[/bold]")
            syntax = Syntax(sparql_query, "sparql", theme="monokai", line_numbers=True)
            console.print(Panel(syntax))
        else:
            print(sparql_query)

        # Save to file if requested
        if output:
            with open(output, 'w') as f:
                f.write(sparql_query)
            console.print(f"\n[green]✓ Query saved to {output}[/green]")

        # Execute if requested
        if execute and executor:
            console.print("\n[yellow]Executing SPARQL query...[/yellow]")
            results = executor.execute_query(sparql_query)

            if isinstance(results, list) and results:
                # Display SELECT results
                table = Table(title="Query Results")

                # Add columns
                for key in results[0].keys():
                    table.add_column(key, style="cyan")

                # Add rows
                for row in results[:20]:  # Limit to 20 rows for display
                    table.add_row(*[str(row.get(k, '')) for k in results[0].keys()])

                console.print(table)

                if len(results) > 20:
                    console.print(f"\n[dim]... and {len(results) - 20} more rows[/dim]")

            elif isinstance(results, bool):
                # UPDATE/DELETE result
                if results:
                    console.print("[green]✓ Query executed successfully[/green]")
                else:
                    console.print("[red]✗ Query execution failed[/red]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--rdf-file', '-r', required=True, help='RDF file to analyze')
@click.option('--format', '-f', default='turtle', help='RDF file format')
@click.option('--output', '-o', help='Output file for schema')
def extract_schema(rdf_file, format, output):
    """Extract relational schema from RDF data"""
    try:
        console.print(f"[blue]Loading RDF file: {rdf_file}[/blue]")

        # Load RDF data
        graph = Graph()
        graph.parse(rdf_file, format=format)

        # Extract schema
        schema_mapper = SchemaMapper(graph)
        schema = schema_mapper.extract_schema()

        # Display schema
        table = Table(title="Extracted Relational Schema")
        table.add_column("Table", style="cyan", no_wrap=True)
        table.add_column("Attributes", style="green")
        table.add_column("Count", style="yellow", justify="right")

        for table_name, attributes in schema.tables.items():
            # Count entities of this type
            count_query = f"""
            SELECT (COUNT(DISTINCT ?s) as ?count)
            WHERE {{ ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/types/{table_name.title()}> }}
            """
            result = graph.query(count_query)
            count = 0
            for row in result:
                count = row[0] if row[0] else 0

            table.add_row(
                table_name,
                ", ".join(attributes),
                str(count)
            )

        console.print(table)

        # Save to file if requested
        if output:
            schema_dict = {
                "tables": schema.tables
            }
            with open(output, 'w') as f:
                json.dump(schema_dict, f, indent=2)
            console.print(f"\n[green]✓ Schema saved to {output}[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--sql-file', '-s', required=True, help='SQL file with queries')
@click.option('--rdf-file', '-r', required=True, help='RDF file for data')
@click.option('--format', '-f', default='turtle', help='RDF file format')
@click.option('--output-dir', '-o', help='Output directory for SPARQL queries')
def batch_convert(sql_file, rdf_file, format, output_dir):
    """Batch convert SQL queries from file"""
    try:
        # Load RDF data
        console.print(f"[blue]Loading RDF file: {rdf_file}[/blue]")
        graph = Graph()
        graph.parse(rdf_file, format=format)

        # Extract schema
        schema_mapper = SchemaMapper(graph)
        schema_mapper.extract_schema()

        # Create converter
        converter = SQL2SPARQLConverter(schema_mapper)

        # Read SQL queries
        with open(sql_file, 'r') as f:
            sql_content = f.read()

        # Split queries (simple split by semicolon)
        queries = [q.strip() for q in sql_content.split(';') if q.strip()]

        console.print(f"\n[yellow]Found {len(queries)} SQL queries[/yellow]")

        # Create output directory if specified
        if output_dir:
            Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Convert each query
        for i, sql_query in enumerate(queries, 1):
            console.print(f"\n[cyan]Query {i}:[/cyan]")

            try:
                # Convert
                sparql_query = converter.convert(sql_query)

                # Display
                console.print(f"[dim]SQL:[/dim] {sql_query[:100]}...")
                console.print(f"[green]✓ Converted successfully[/green]")

                # Save if output directory specified
                if output_dir:
                    output_file = Path(output_dir) / f"query_{i}.sparql"
                    with open(output_file, 'w') as f:
                        f.write(sparql_query)

            except Exception as e:
                console.print(f"[red]✗ Conversion failed: {e}[/red]")

        if output_dir:
            console.print(f"\n[green]✓ SPARQL queries saved to {output_dir}[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--endpoint', '-e', required=True, help='SPARQL endpoint URL')
@click.option('--username', '-u', help='Username for authentication')
@click.option('--password', '-p', help='Password for authentication')
@click.option('--store-type', '-t',
              type=click.Choice(['allegrograph', 'fuseki', 'blazegraph', 'virtuoso']),
              default='fuseki',
              help='RDF store type')
def test_connection(endpoint, username, password, store_type):
    """Test connection to SPARQL endpoint"""
    try:
        console.print(f"[blue]Testing connection to {endpoint}[/blue]")

        # Create executor
        executor = SPARQLExecutor(
            store_type=StoreType[store_type.upper()],
            endpoint=endpoint,
            username=username,
            password=password
        )

        # Test connection
        if executor.test_connection():
            console.print("[green]✓ Connection successful[/green]")

            # Get statistics
            stats = executor.get_statistics()

            table = Table(title="Store Statistics")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="yellow", justify="right")

            for key, value in stats.items():
                table.add_row(key.replace('_', ' ').title(), f"{value:,}")

            console.print(table)
        else:
            console.print("[red]✗ Connection failed[/red]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
def examples():
    """Show example SQL queries and their SPARQL equivalents"""
    examples = [
        {
            "title": "Simple SELECT",
            "sql": "SELECT client.name, client.email FROM client",
            "sparql": """SELECT ?name ?email
WHERE {
  ?s0 rdf:type <http://example.org/types/Client> .
  ?s0 <http://example.org/ontology/name> ?name .
  ?s0 <http://example.org/ontology/email> ?email .
}"""
        },
        {
            "title": "SELECT with WHERE",
            "sql": "SELECT product.name, product.price FROM product WHERE product.price > 100",
            "sparql": """SELECT ?name ?price
WHERE {
  ?s0 rdf:type <http://example.org/types/Product> .
  ?s0 <http://example.org/ontology/name> ?name .
  ?s0 <http://example.org/ontology/price> ?price .
  FILTER(?price > 100)
}"""
        },
        {
            "title": "JOIN Query",
            "sql": """SELECT client.name, order.date
FROM client, order
WHERE client.id = order.client_id""",
            "sparql": """SELECT ?name ?date
WHERE {
  ?s0 rdf:type <http://example.org/types/Client> .
  ?s0 <http://example.org/ontology/name> ?name .
  ?s1 rdf:type <http://example.org/types/Order> .
  ?s1 <http://example.org/ontology/date> ?date .
  ?s1 <http://example.org/ontology/client_id> ?s0 .
}"""
        },
        {
            "title": "Aggregate Query",
            "sql": "SELECT COUNT(product.name) FROM product GROUP BY product.category",
            "sparql": """SELECT (COUNT(?name) AS ?name_count) ?category
WHERE {
  ?s0 rdf:type <http://example.org/types/Product> .
  ?s0 <http://example.org/ontology/name> ?name .
  ?s0 <http://example.org/ontology/category> ?category .
}
GROUP BY ?category"""
        }
    ]

    for example in examples:
        console.print(f"\n[bold cyan]{example['title']}[/bold cyan]")
        console.print("\n[bold]SQL:[/bold]")
        syntax = Syntax(example['sql'], "sql", theme="monokai")
        console.print(Panel(syntax))
        console.print("\n[bold]SPARQL:[/bold]")
        syntax = Syntax(example['sparql'], "sparql", theme="monokai")
        console.print(Panel(syntax))


if __name__ == '__main__':
    cli()
