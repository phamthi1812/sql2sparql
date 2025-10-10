# SPARQL Endpoint Setup

This guide explains how to set up a SPARQL endpoint for testing the SQL2SPARQL converter with real datasets.

## Apache Jena Fuseki Setup

### 1. Download Apache Jena Fuseki

```bash
# Download Apache Jena Fuseki 5.5.0
wget https://downloads.apache.org/jena/binaries/apache-jena-fuseki-5.5.0.tar.gz

# Extract the archive
tar -xzf apache-jena-fuseki-5.5.0.tar.gz

# Navigate to the Fuseki directory
cd apache-jena-fuseki-5.5.0
```

### 2. Start Fuseki Server

```bash
# Create a directory for Fuseki databases
mkdir -p ../fuseki-databases

# Start Fuseki with update support
./fuseki-server --loc=../fuseki-databases --update /northwind

# Or start on a specific port
./fuseki-server --port=3030 --loc=../fuseki-databases --update /northwind
```

### 3. Access Fuseki

- **Web Interface**: http://localhost:3030
- **SPARQL Endpoint**: http://localhost:3030/northwind/sparql
- **Graph Store**: http://localhost:3030/northwind/data

## Loading Northwind Dataset

### 1. Load the Dataset

```bash
# From the project root directory
curl -X POST -H "Content-Type: text/turtle" \
  --data-binary @datasets/northwind.ttl \
  http://localhost:3030/northwind/data
```

### 2. Verify Data Loading

```bash
# Check triple count
curl -s -H "Accept: application/sparql-results+json" \
  --data-urlencode "query=SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }" \
  http://localhost:3030/northwind/sparql
```

Expected response: `{"count": "514"}` (approximately)

## Testing the Setup

### 1. Simple Test Query

```bash
curl -s -H "Accept: application/sparql-results+json" \
  --data-urlencode "query=SELECT ?companyName ?country WHERE { ?s <http://northwind.example.org/ontology/companyName> ?companyName . ?s <http://northwind.example.org/ontology/country> ?country . } LIMIT 5" \
  http://localhost:3030/northwind/sparql
```

### 2. Run SQL2SPARQL Tests

```bash
# Test with Northwind dataset
cd datasets
python test_northwind.py

# Test simple GROUP BY queries
python test_simple_groupby.py

# Test complex aggregate queries
python test_complex_aggregates.py
```

## Alternative SPARQL Endpoints

If you prefer not to install Fuseki locally, you can use:

### 1. RDFLib (In-Memory)

```python
from sql2sparql.executors.sparql_executor import SPARQLExecutor, StoreType

# Use in-memory RDFLib store
executor = SPARQLExecutor(StoreType.RDFLIB)
```

### 2. Public SPARQL Endpoints

- **DBpedia**: http://dbpedia.org/sparql
- **Wikidata**: https://query.wikidata.org/sparql

## Troubleshooting

### Port Already in Use

```bash
# Check what's using port 3030
lsof -i :3030

# Kill the process
kill -9 <PID>

# Or use a different port
./fuseki-server --port=3031 --loc=../fuseki-databases --update /northwind
```

### Java Not Found

```bash
# Install Java (macOS)
brew install openjdk@11

# Set JAVA_HOME
export JAVA_HOME=/usr/local/opt/openjdk@11
```

### Permission Issues

```bash
# Make Fuseki executable
chmod +x apache-jena-fuseki-5.5.0/fuseki-server
```

## Stopping Fuseki

```bash
# Find the Fuseki process
ps aux | grep fuseki

# Kill the process
kill -9 <PID>
```

## Configuration

Fuseki configuration files are located in:
- `apache-jena-fuseki-5.5.0/run/config.ttl`
- `apache-jena-fuseki-5.5.0/run/shiro.ini`

For advanced configuration, see the [Apache Jena Fuseki Documentation](https://jena.apache.org/documentation/fuseki2/).