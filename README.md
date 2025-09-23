# SQL2SPARQL - Automatic SQL to SPARQL Converter

A production-ready implementation of the SQL2SPARQL4RDF framework for automatic conversion of SQL queries to SPARQL queries, enabling direct querying of RDF data using familiar SQL syntax.

Based on the research paper:
> **SQL2SPARQL4RDF: Automatic SQL to SPARQL Conversion for RDF Querying**
> Ahmed ABATAL, Khadija Alaoui, Mohamed Bahaj, Larbi Alaoui
> *International Journal of Advanced Computer Science and Applications (IJACSA)*, Vol. 10, No. 11, 2019
> DOI: [10.1145/3372938.3372968](https://doi.org/10.1145/3372938.3372968)

## Features

✅ **Complete SQL Support**
- SELECT queries with JOIN, WHERE, GROUP BY, HAVING, ORDER BY
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- INSERT and DELETE operations
- UNION, INTERSECT, EXCEPT combinations

✅ **RDF Integration**
- Automatic schema extraction from RDF data
- Support for multiple RDF formats (Turtle, RDF/XML, N3, JSON-LD)
- Integration with popular RDF stores (AllegroGraph, Fuseki, Blazegraph, Virtuoso)
- In-memory processing with RDFLib

✅ **Developer Friendly**
- Rich CLI interface with syntax highlighting
- Python API for integration
- Comprehensive test suite
- Detailed logging and error messages

## Installation

```bash
# Install from source
git clone https://github.com/yourusername/sql2sparql
cd sql2sparql
pip install -e .

# Or install with pip
pip install sql2sparql
```

## Quick Start

### Command Line Usage

```bash
# Convert a simple SQL query
sql2sparql convert -s "SELECT name, email FROM client" -r data.ttl

# Extract schema from RDF data
sql2sparql extract-schema -r data.ttl -o schema.json

# Batch convert SQL queries
sql2sparql batch-convert -s queries.sql -r data.ttl -o output/

# Test connection to SPARQL endpoint
sql2sparql test-connection -e http://localhost:3030/sparql

# Show examples
sql2sparql examples
```

### Python API Usage

```python
from sql2sparql import SQL2SPARQLConverter, SchemaMapper, SPARQLExecutor
from sql2sparql.executors.sparql_executor import StoreType
from rdflib import Graph

# Load RDF data
graph = Graph()
graph.parse("data.ttl", format="turtle")

# Extract schema
schema_mapper = SchemaMapper(graph)
schema = schema_mapper.extract_schema()

# Create converter
converter = SQL2SPARQLConverter(schema_mapper)

# Convert SQL to SPARQL
sql_query = "SELECT name, email FROM client WHERE age > 25"
sparql_query = converter.convert(sql_query)
print(sparql_query)

# Execute SPARQL query
executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=graph)
results = executor.execute_query(sparql_query)
for row in results:
    print(row)
```

## Supported SQL Constructs

### SELECT Queries
```sql
-- Simple SELECT
SELECT name, email FROM client

-- With WHERE clause
SELECT * FROM product WHERE price > 100

-- With JOIN
SELECT client.name, order.date
FROM client, order
WHERE client.id = order.client_id

-- With aggregates
SELECT category, COUNT(*), AVG(price)
FROM product
GROUP BY category
HAVING COUNT(*) > 5

-- With ORDER BY and LIMIT
SELECT name, price FROM product
ORDER BY price DESC
LIMIT 10
```

### INSERT Queries
```sql
INSERT INTO client (name, email, age)
VALUES ('John Doe', 'john@example.com', 30)
```

### DELETE Queries
```sql
DELETE FROM client WHERE age < 18
```

## Architecture

The system follows a modular architecture:

```
sql2sparql/
├── core/
│   ├── converter.py      # Main converter orchestrator
│   ├── schema_mapper.py  # RDF schema extraction
│   └── models.py         # Data models
├── parsers/
│   └── sql_parser.py     # SQL query parser
├── converters/
│   ├── select_converter.py      # SELECT clause converter
│   ├── where_converter.py       # WHERE clause converter
│   ├── group_having_converter.py # GROUP BY/HAVING converter
│   └── insert_delete_converter.py # INSERT/DELETE converter
├── executors/
│   └── sparql_executor.py       # SPARQL execution engine
└── cli/
    └── main.py                   # CLI interface
```

## Conversion Algorithm

The conversion follows these steps:

1. **Schema Extraction**: Analyze RDF data to extract a relational schema
2. **SQL Parsing**: Parse SQL query into structured components
3. **Pattern Generation**: Convert SQL elements to SPARQL triple patterns
4. **Query Construction**: Assemble SPARQL query with proper syntax
5. **Execution**: Run SPARQL on RDF store and return results

## RDF Store Integration

### AllegroGraph
```python
executor = SPARQLExecutor(
    store_type=StoreType.ALLEGROGRAPH,
    endpoint="http://localhost:10035/repositories/myrepo/sparql",
    username="user",
    password="pass"
)
```

### Apache Jena Fuseki
```python
executor = SPARQLExecutor(
    store_type=StoreType.FUSEKI,
    endpoint="http://localhost:3030/dataset/sparql"
)
```

### Blazegraph
```python
executor = SPARQLExecutor(
    store_type=StoreType.BLAZEGRAPH,
    endpoint="http://localhost:9999/blazegraph/sparql"
)
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=sql2sparql

# Run specific test module
pytest tests/test_converter.py
```

## Performance Considerations

- **Schema Caching**: Schema extraction is cached after first run
- **Query Optimization**: Generated SPARQL queries are optimized for execution
- **Batch Processing**: Support for batch conversion of multiple queries
- **Connection Pooling**: Reuse connections to remote SPARQL endpoints

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Citation

If you use this software in your research, please cite:

```bibtex
@inproceedings{abatal2019sql2sparql,
  title={SQL2SPARQL4RDF: Automatic SQL to SPARQL Conversion for RDF Querying},
  author={Abatal, Ahmed and Alaoui, Khadija and Bahaj, Mohamed and Alaoui, Larbi},
  booktitle={International Conference on Advanced Computer Science and Applications},
  year={2019},
  doi={10.1145/3372938.3372968}
}
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

This implementation is based on the research paper:

**SQL2SPARQL4RDF: Automatic SQL to SPARQL Conversion for RDF Querying**
- Authors: Ahmed ABATAL¹, Khadija Alaoui², Mohamed Bahaj⁴, Larbi Alaoui³
- ¹²⁴ Mathematics and Computer Science Department, Hassan I University, Faculty of Sciences and Techniques, Settat, Morocco
- ³ Mathematics and Computer Science Department, International University of Rabat, Sala Al Jadida, Morocco
- Published in: International Journal of Advanced Computer Science and Applications (IJACSA), Vol. 10, No. 11, 2019
- Conference: October 2019
- DOI: 10.1145/3372938.3372968
- Citations: 3 | Reads: 1,100+

Special thanks to the authors for their comprehensive algorithms and framework design that made this implementation possible.

## Support

For issues, questions, or suggestions, please:
- Open an issue on GitHub
- Contact the maintainers
- Check the documentation

## Roadmap

- [ ] Support for more complex SQL constructs (subqueries, CTEs)
- [ ] Query optimization engine
- [ ] GraphQL interface
- [ ] Docker containerization
- [ ] Web UI for interactive conversion
- [ ] Support for additional RDF stores
- [ ] Performance benchmarking suite