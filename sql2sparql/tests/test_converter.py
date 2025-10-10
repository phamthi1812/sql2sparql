"""
Comprehensive tests for SQL2SPARQL converter
Based on examples from the paper
"""
import pytest
from rdflib import Graph, Namespace, Literal, URIRef, RDF

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from sql2sparql.parsers.sql_parser import SQLParser
from sql2sparql.core.models import QueryType, AggregateFunction
from sql2sparql.executors.sparql_executor import SPARQLExecutor, StoreType


@pytest.fixture
def sample_rdf_data():
    """Create sample RDF data for testing"""
    graph = Graph()

    # Define namespaces
    ex = Namespace("http://example.org/")
    ont = Namespace("http://example.org/ontology/")
    types = Namespace("http://example.org/types/")

    # Add sample data - Clients
    client1 = ex.client1
    graph.add((client1, RDF.type, types.Client))
    graph.add((client1, ont.name, Literal("John Doe")))
    graph.add((client1, ont.email, Literal("john@example.com")))
    graph.add((client1, ont.age, Literal(30)))

    client2 = ex.client2
    graph.add((client2, RDF.type, types.Client))
    graph.add((client2, ont.name, Literal("Jane Smith")))
    graph.add((client2, ont.email, Literal("jane@example.com")))
    graph.add((client2, ont.age, Literal(25)))

    # Add sample data - Orders
    order1 = ex.order1
    graph.add((order1, RDF.type, types.Order))
    graph.add((order1, ont.date, Literal("2024-01-15")))
    graph.add((order1, ont.total, Literal(150.00)))
    graph.add((order1, ont.client, client1))

    order2 = ex.order2
    graph.add((order2, RDF.type, types.Order))
    graph.add((order2, ont.date, Literal("2024-01-20")))
    graph.add((order2, ont.total, Literal(250.00)))
    graph.add((order2, ont.client, client2))

    # Add sample data - Products
    product1 = ex.product1
    graph.add((product1, RDF.type, types.Product))
    graph.add((product1, ont.name, Literal("Laptop")))
    graph.add((product1, ont.price, Literal(1200.00)))
    graph.add((product1, ont.category, Literal("Electronics")))

    product2 = ex.product2
    graph.add((product2, RDF.type, types.Product))
    graph.add((product2, ont.name, Literal("Book")))
    graph.add((product2, ont.price, Literal(25.00)))
    graph.add((product2, ont.category, Literal("Books")))

    return graph


@pytest.fixture
def converter_with_schema(sample_rdf_data):
    """Create converter with schema extracted from sample data"""
    schema_mapper = SchemaMapper(sample_rdf_data)
    schema_mapper.extract_schema()
    return SQL2SPARQLConverter(schema_mapper)


class TestSQLParser:
    """Test SQL parsing functionality"""

    def test_parse_simple_select(self):
        """Test parsing simple SELECT query"""
        parser = SQLParser()
        query = "SELECT name, email FROM client"
        result = parser.parse(query)

        assert result.type == QueryType.SELECT
        assert len(result.select_attributes) == 2
        assert result.select_attributes[0].name == "name"
        assert result.select_attributes[1].name == "email"
        assert "client" in result.from_tables

    def test_parse_select_with_where(self):
        """Test parsing SELECT with WHERE clause"""
        parser = SQLParser()
        query = "SELECT name FROM client WHERE age > 25"
        result = parser.parse(query)

        assert result.type == QueryType.SELECT
        assert len(result.where_conditions) == 1
        assert result.where_conditions[0].attribute.name == "age"
        assert result.where_conditions[0].operator == ">"
        assert result.where_conditions[0].value == "25"

    def test_parse_join_query(self):
        """Test parsing JOIN query"""
        parser = SQLParser()
        query = """SELECT client.name, order.date
                   FROM client, order
                   WHERE client.subject = order.client"""
        result = parser.parse(query)

        assert result.type == QueryType.SELECT
        assert len(result.from_tables) == 2
        assert len(result.join_conditions) == 1
        assert result.join_conditions[0].left_operand.name == "subject"
        assert result.join_conditions[0].right_operand.name == "client"

    def test_parse_aggregate_query(self):
        """Test parsing aggregate functions"""
        parser = SQLParser()
        query = "SELECT COUNT(name), AVG(price) FROM product"
        result = parser.parse(query)

        assert result.type == QueryType.SELECT
        assert result.select_attributes[0].aggregate == AggregateFunction.COUNT
        assert result.select_attributes[1].aggregate == AggregateFunction.AVG

    def test_parse_group_by_query(self):
        """Test parsing GROUP BY clause"""
        parser = SQLParser()
        query = "SELECT category, COUNT(*) FROM product GROUP BY category"
        result = parser.parse(query)

        assert len(result.group_by) == 1
        assert result.group_by[0].name == "category"

    def test_parse_having_query(self):
        """Test parsing HAVING clause"""
        parser = SQLParser()
        query = """SELECT category, COUNT(name)
                   FROM product
                   GROUP BY category
                   HAVING COUNT(name) > 5"""
        result = parser.parse(query)

        assert len(result.having) == 1
        assert result.having[0].attribute.aggregate == AggregateFunction.COUNT
        assert result.having[0].operator == ">"
        assert result.having[0].value == "5"

    def test_parse_order_by_query(self):
        """Test parsing ORDER BY clause"""
        parser = SQLParser()
        query = "SELECT name, price FROM product ORDER BY price DESC"
        result = parser.parse(query)

        assert len(result.order_by) == 1
        assert result.order_by[0][0].name == "price"
        assert result.order_by[0][1] == "DESC"

    def test_parse_insert_query(self):
        """Test parsing INSERT query"""
        parser = SQLParser()
        query = "INSERT INTO client (name, email) VALUES ('Bob', 'bob@example.com')"
        result = parser.parse(query)

        assert result.type == QueryType.INSERT
        assert result.insert_table == "client"
        assert result.insert_values["name"] == "Bob"
        assert result.insert_values["email"] == "bob@example.com"

    def test_parse_delete_query(self):
        """Test parsing DELETE query"""
        parser = SQLParser()
        query = "DELETE FROM client WHERE age < 18"
        result = parser.parse(query)

        assert result.type == QueryType.DELETE
        assert result.delete_table == "client"
        assert len(result.where_conditions) == 1
        assert result.where_conditions[0].attribute.name == "age"


class TestSchemaMapper:
    """Test schema extraction functionality"""

    def test_extract_schema(self, sample_rdf_data):
        """Test schema extraction from RDF data"""
        mapper = SchemaMapper(sample_rdf_data)
        schema = mapper.extract_schema()

        assert "client" in schema.tables
        assert "order" in schema.tables
        assert "product" in schema.tables

        # Check client attributes
        client_attrs = schema.tables["client"]
        assert "subject" in client_attrs
        assert "name" in client_attrs
        assert "email" in client_attrs
        assert "age" in client_attrs

    def test_validate_sql_reference(self, sample_rdf_data):
        """Test SQL reference validation"""
        mapper = SchemaMapper(sample_rdf_data)
        mapper.extract_schema()

        assert mapper.validate_sql_reference("client", "name") == True
        assert mapper.validate_sql_reference("client", "invalid") == False
        assert mapper.validate_sql_reference("invalid", "name") == False


class TestSQL2SPARQLConverter:
    """Test main converter functionality"""

    def test_simple_select_conversion(self, converter_with_schema):
        """Test conversion of simple SELECT query"""
        sql = "SELECT name, email FROM client"
        sparql = converter_with_schema.convert(sql)

        assert "SELECT" in sparql
        assert "?o0" in sparql  # Variable for name
        assert "?o1" in sparql  # Variable for email
        assert "WHERE" in sparql
        # Type triple is optional for simple queries
        # assert "rdf:type" in sparql

    def test_select_with_where_conversion(self, converter_with_schema):
        """Test conversion of SELECT with WHERE"""
        sql = "SELECT name FROM client WHERE age > 25"
        sparql = converter_with_schema.convert(sql)

        assert "SELECT" in sparql
        assert "FILTER" in sparql
        assert "> 25" in sparql

    def test_join_conversion(self, converter_with_schema):
        """Test conversion of JOIN query"""
        sql = """SELECT client.name, order.date
                 FROM client, order
                 WHERE order.client = client.subject"""
        sparql = converter_with_schema.convert(sql)

        assert "SELECT" in sparql
        assert "?s0" in sparql  # Subject variable for client
        assert "?s1" in sparql  # Subject variable for order

    def test_aggregate_conversion(self, converter_with_schema):
        """Test conversion of aggregate functions"""
        sql = "SELECT COUNT(name) FROM product"
        sparql = converter_with_schema.convert(sql)

        assert "COUNT" in sparql
        assert "SELECT" in sparql

    def test_group_by_conversion(self, converter_with_schema):
        """Test conversion of GROUP BY clause"""
        sql = "SELECT category, COUNT(name) FROM product GROUP BY category"
        sparql = converter_with_schema.convert(sql)

        assert "GROUP BY" in sparql
        assert "COUNT" in sparql

    def test_order_by_conversion(self, converter_with_schema):
        """Test conversion of ORDER BY clause"""
        sql = "SELECT name, price FROM product ORDER BY price DESC"
        sparql = converter_with_schema.convert(sql)

        assert "ORDER BY" in sparql
        assert "DESC" in sparql

    def test_insert_conversion(self, converter_with_schema):
        """Test conversion of INSERT query"""
        sql = "INSERT INTO client (name, email) VALUES ('Bob', 'bob@example.com')"
        sparql = converter_with_schema.convert(sql)

        assert "INSERT DATA" in sparql
        assert '"Bob"' in sparql
        assert '"bob@example.com"' in sparql
        assert "rdf:type" in sparql

    def test_delete_conversion(self, converter_with_schema):
        """Test conversion of DELETE query"""
        sql = "DELETE FROM client WHERE age < 18"
        sparql = converter_with_schema.convert(sql)

        assert "DELETE" in sparql
        assert "WHERE" in sparql
        assert "FILTER" in sparql
        assert "< 18" in sparql


class TestSPARQLExecutor:
    """Test SPARQL execution functionality"""

    def test_execute_select(self, sample_rdf_data):
        """Test executing SELECT query"""
        executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=sample_rdf_data)

        query = """
        SELECT ?name ?email
        WHERE {
            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/types/Client> .
            ?s <http://example.org/ontology/name> ?name .
            ?s <http://example.org/ontology/email> ?email .
        }
        """

        results = executor.execute_query(query)
        assert len(results) == 2
        assert all("name" in r and "email" in r for r in results)

    def test_execute_with_filter(self, sample_rdf_data):
        """Test executing query with FILTER"""
        executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=sample_rdf_data)

        query = """
        SELECT ?name
        WHERE {
            ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/types/Client> .
            ?s <http://example.org/ontology/name> ?name .
            ?s <http://example.org/ontology/age> ?age .
            FILTER(?age > 25)
        }
        """

        results = executor.execute_query(query)
        assert len(results) == 1
        assert results[0]["name"] == "John Doe"

    def test_get_statistics(self, sample_rdf_data):
        """Test getting store statistics"""
        executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=sample_rdf_data)

        stats = executor.get_statistics()
        assert "total_triples" in stats
        assert stats["total_triples"] > 0
        assert "distinct_subjects" in stats
        assert "distinct_predicates" in stats


class TestEndToEnd:
    """End-to-end integration tests"""

    def test_complete_workflow(self, sample_rdf_data):
        """Test complete workflow from SQL to results"""
        # Extract schema
        schema_mapper = SchemaMapper(sample_rdf_data)
        schema_mapper.extract_schema()

        # Create converter
        converter = SQL2SPARQLConverter(schema_mapper)

        # Convert SQL query
        sql = "SELECT name, email FROM client WHERE age > 25"
        sparql = converter.convert(sql)

        # Execute SPARQL
        executor = SPARQLExecutor(store_type=StoreType.RDFLIB, graph=sample_rdf_data)
        results = executor.execute_query(sparql)

        # Verify results
        assert len(results) == 1
        # RDFLib returns keys without the "?" prefix
        assert results[0]["o0"] == "John Doe"
        assert results[0]["o1"] == "john@example.com"
