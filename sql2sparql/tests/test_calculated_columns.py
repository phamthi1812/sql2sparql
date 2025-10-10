#!/usr/bin/env python3
"""
Unit tests for calculated columns feature
"""

import unittest
from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from rdflib import Graph


class TestCalculatedColumns(unittest.TestCase):
    """Test suite for calculated columns in SQL2SPARQL conversion"""

    def setUp(self):
        """Set up test fixtures"""
        # Create a simple RDF graph
        self.graph = Graph()
        self.schema_mapper = SchemaMapper(self.graph)
        self.converter = SQL2SPARQLConverter(self.schema_mapper)

    def test_simple_calculated_column(self):
        """Test simple calculated column conversion"""
        sql = "SELECT name, price * stock AS inventory_value FROM product"
        sparql = self.converter.convert(sql)

        # Check that SPARQL contains the calculation
        self.assertIn("?price * ?stock", sparql)
        self.assertIn("AS ?inventory_value", sparql)
        # Check triple patterns are generated
        self.assertIn("?product <http://example.org/ontology/name> ?name", sparql)
        self.assertIn("?product <http://example.org/ontology/price> ?price", sparql)
        self.assertIn("?product <http://example.org/ontology/stock> ?stock", sparql)

    def test_calculated_column_with_literal(self):
        """Test calculated column with literal value"""
        sql = "SELECT name, price * 1.1 AS price_with_tax FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn("?price * 1.1", sparql)
        self.assertIn("AS ?price_with_tax", sparql)

    def test_multiple_calculated_columns(self):
        """Test multiple calculated columns in one query"""
        sql = "SELECT name, price * 1.1 AS price_with_tax, stock - 5 AS adjusted_stock FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn("?price * 1.1", sparql)
        self.assertIn("AS ?price_with_tax", sparql)
        self.assertIn("?stock - 5", sparql)
        self.assertIn("AS ?adjusted_stock", sparql)

    def test_calculated_column_in_where(self):
        """Test calculated column in WHERE clause"""
        sql = "SELECT name, price, stock FROM product WHERE (price * stock) > 1000"
        sparql = self.converter.convert(sql)

        # Check the filter is created
        self.assertIn("FILTER((?price * ?stock) > 1000)", sparql)
        # Check triple patterns exist (subject variable name may vary)
        self.assertIn("<http://example.org/ontology/price> ?price", sparql)
        self.assertIn("<http://example.org/ontology/stock> ?stock", sparql)

    def test_complex_calculation(self):
        """Test complex calculation with multiple operators"""
        sql = "SELECT name, (price * stock) / 2 AS half_value FROM product"
        sparql = self.converter.convert(sql)

        # Should contain the complex expression
        self.assertIn("AS ?half_value", sparql)
        # Check that variables are created
        self.assertIn("?price", sparql)
        self.assertIn("?stock", sparql)

    def test_calculated_with_table_prefix(self):
        """Test calculated column with table prefixes"""
        sql = "SELECT product.name, product.price * product.stock AS value FROM product"
        sparql = self.converter.convert(sql)

        # Should handle table prefixes
        self.assertIn("?product_price * ?product_stock", sparql)
        self.assertIn("AS ?value", sparql)

    def test_addition_calculation(self):
        """Test addition in calculated column"""
        sql = "SELECT name, price + 50 AS increased_price FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn("?price + 50", sparql)
        self.assertIn("AS ?increased_price", sparql)

    def test_subtraction_calculation(self):
        """Test subtraction in calculated column"""
        sql = "SELECT name, stock - 10 AS reduced_stock FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn("?stock - 10", sparql)
        self.assertIn("AS ?reduced_stock", sparql)

    def test_division_calculation(self):
        """Test division in calculated column"""
        sql = "SELECT name, price / 2 AS half_price FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn("?price / 2", sparql)
        self.assertIn("AS ?half_price", sparql)

    def test_parenthesized_calculation(self):
        """Test calculation with parentheses"""
        sql = "SELECT name, (price + 10) * stock AS adjusted_value FROM product"
        sparql = self.converter.convert(sql)

        # Should preserve the calculation structure
        self.assertIn("AS ?adjusted_value", sparql)
        self.assertIn("?price", sparql)
        self.assertIn("?stock", sparql)

    def test_no_alias_calculated_column(self):
        """Test calculated column without explicit alias"""
        sql = "SELECT name, price * stock FROM product"
        sparql = self.converter.convert(sql)

        # Should generate an automatic alias
        self.assertIn("?price * ?stock", sparql)
        self.assertIn("AS ?calc_", sparql)


if __name__ == '__main__':
    unittest.main()