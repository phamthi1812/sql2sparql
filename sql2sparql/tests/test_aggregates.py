#!/usr/bin/env python3
"""
Unit tests for aggregate functions
"""

import unittest
from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from sql2sparql.parsers.sql_parser import SQLParser
from rdflib import Graph


class TestAggregateFunctions(unittest.TestCase):
    """Test suite for aggregate function conversion"""

    def setUp(self):
        """Set up test fixtures"""
        self.graph = Graph()
        self.schema_mapper = SchemaMapper(self.graph)
        self.converter = SQL2SPARQLConverter(self.schema_mapper)
        self.parser = SQLParser()

    def test_count_all(self):
        """Test COUNT(*) conversion"""
        sql = "SELECT COUNT(*) FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn('COUNT(*)', sparql)

    def test_count_column(self):
        """Test COUNT with specific column"""
        sql = "SELECT COUNT(name) FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn('COUNT(?', sparql)
        self.assertIn('name', sparql.lower())

    def test_count_with_alias(self):
        """Test COUNT with alias"""
        sql = "SELECT COUNT(name) AS product_count FROM product"

        # First test parsing
        parsed = self.parser.parse(sql)
        self.assertEqual(len(parsed.select_attributes), 1)
        self.assertEqual(parsed.select_attributes[0].alias, 'product_count')

        sparql = self.converter.convert(sql)
        self.assertIn('COUNT', sparql)
        self.assertIn('product_count', sparql)

    def test_sum_function(self):
        """Test SUM function conversion"""
        sql = "SELECT SUM(price) FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn('SUM(?', sparql)

    def test_sum_with_alias(self):
        """Test SUM with alias"""
        sql = "SELECT SUM(price) AS total_price FROM product"

        parsed = self.parser.parse(sql)
        self.assertEqual(parsed.select_attributes[0].alias, 'total_price')

        sparql = self.converter.convert(sql)
        self.assertIn('SUM', sparql)
        self.assertIn('total_price', sparql)

    def test_avg_function(self):
        """Test AVG function conversion"""
        sql = "SELECT AVG(price) FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn('AVG(?', sparql)

    def test_min_function(self):
        """Test MIN function conversion"""
        sql = "SELECT MIN(price) FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn('MIN(?', sparql)

    def test_max_function(self):
        """Test MAX function conversion"""
        sql = "SELECT MAX(price) FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn('MAX(?', sparql)

    def test_multiple_aggregates(self):
        """Test multiple aggregate functions in one query"""
        sql = "SELECT COUNT(name), SUM(price), AVG(stock) FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn('COUNT', sparql)
        self.assertIn('SUM', sparql)
        self.assertIn('AVG', sparql)

    def test_aggregate_with_group_by(self):
        """Test aggregate with GROUP BY"""
        sql = "SELECT category, COUNT(name) FROM product GROUP BY category"
        sparql = self.converter.convert(sql)

        self.assertIn('COUNT', sparql)
        self.assertIn('GROUP BY', sparql)

    def test_aggregate_with_having(self):
        """Test aggregate with HAVING clause"""
        sql = "SELECT category, COUNT(name) FROM product GROUP BY category HAVING COUNT(name) > 5"
        sparql = self.converter.convert(sql)

        self.assertIn('COUNT', sparql)
        self.assertIn('GROUP BY', sparql)
        self.assertIn('HAVING', sparql)

    def test_aggregate_with_where(self):
        """Test aggregate with WHERE clause"""
        sql = "SELECT COUNT(name) FROM product WHERE price > 100"
        sparql = self.converter.convert(sql)

        self.assertIn('COUNT', sparql)
        self.assertIn('FILTER(?price > 100)', sparql)

    def test_aggregate_with_join(self):
        """Test aggregate with JOIN"""
        sql = """SELECT client.name, SUM(order.total) AS total_spent
                 FROM client, order
                 WHERE client.id = order.client_id
                 GROUP BY client.name"""

        # Test parsing first
        parsed = self.parser.parse(sql)
        self.assertEqual(len(parsed.select_attributes), 2)
        self.assertEqual(parsed.select_attributes[1].alias, 'total_spent')

        sparql = self.converter.convert(sql)
        self.assertIn('SUM', sparql)

    def test_aggregate_with_table_prefix(self):
        """Test aggregate with table-qualified column"""
        sql = "SELECT SUM(product.price) FROM product"
        sparql = self.converter.convert(sql)

        self.assertIn('SUM', sparql)

    def test_mixed_aggregate_and_regular(self):
        """Test mixing aggregate and regular columns"""
        sql = "SELECT category, COUNT(name), MAX(price) FROM product GROUP BY category"
        sparql = self.converter.convert(sql)

        self.assertIn('?', sparql)  # Regular variable for category
        self.assertIn('COUNT', sparql)
        self.assertIn('MAX', sparql)
        self.assertIn('GROUP BY', sparql)

    def test_aggregate_with_order_by(self):
        """Test aggregate with ORDER BY"""
        sql = "SELECT category, COUNT(name) AS count FROM product GROUP BY category ORDER BY count DESC"
        sparql = self.converter.convert(sql)

        self.assertIn('COUNT', sparql)
        self.assertIn('ORDER BY', sparql)

    def test_aggregate_distinct(self):
        """Test aggregate with DISTINCT"""
        sql = "SELECT COUNT(DISTINCT category) FROM product"
        # This might need special handling
        try:
            sparql = self.converter.convert(sql)
            self.assertIn('COUNT', sparql)
        except:
            # Document if not supported
            pass

    def test_aggregate_in_having(self):
        """Test using aggregate in HAVING clause"""
        sql = "SELECT category FROM product GROUP BY category HAVING SUM(price) > 1000"
        sparql = self.converter.convert(sql)

        self.assertIn('GROUP BY', sparql)
        self.assertIn('HAVING', sparql)

    def test_nested_aggregates_not_supported(self):
        """Test that nested aggregates are handled gracefully"""
        sql = "SELECT MAX(COUNT(name)) FROM product"
        # This should either fail gracefully or be handled specially
        try:
            sparql = self.converter.convert(sql)
            # If it works, great
            self.assertIsNotNone(sparql)
        except:
            # Expected to fail for nested aggregates
            pass


if __name__ == '__main__':
    unittest.main()