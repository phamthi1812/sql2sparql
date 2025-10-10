#!/usr/bin/env python3
"""
Unit tests for UNION query handling
"""

import unittest
from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from rdflib import Graph


class TestUnionQueries(unittest.TestCase):
    """Test suite for UNION query conversion"""

    def setUp(self):
        """Set up test fixtures"""
        self.graph = Graph()
        self.schema_mapper = SchemaMapper(self.graph)
        self.converter = SQL2SPARQLConverter(self.schema_mapper)

    def test_simple_union(self):
        """Test simple UNION of two SELECT queries"""
        sql = """SELECT name FROM client
                 UNION
                 SELECT name FROM product"""
        sparql = self.converter.convert(sql)

        # Should create SPARQL UNION structure
        self.assertIn('UNION', sparql)
        self.assertIn('?s0 <http://example.org/ontology/name> ?o0', sparql)
        # Should have two blocks with curly braces
        self.assertEqual(sparql.count('{'), 3)  # Main WHERE { and two sub-blocks
        self.assertEqual(sparql.count('}'), 3)

    def test_union_with_multiple_columns(self):
        """Test UNION with multiple columns"""
        sql = """SELECT name, email FROM client
                 UNION
                 SELECT name, description FROM product"""
        sparql = self.converter.convert(sql)

        self.assertIn('UNION', sparql)
        # Should select same number of variables
        self.assertIn('SELECT ?o0 ?o1', sparql)

    def test_three_way_union(self):
        """Test UNION of three queries"""
        sql = """SELECT name FROM client
                 UNION
                 SELECT name FROM product
                 UNION
                 SELECT name FROM supplier"""
        sparql = self.converter.convert(sql)

        # Should have two UNION keywords for three queries
        self.assertEqual(sparql.count('UNION'), 2)

    def test_union_with_where_clause(self):
        """Test UNION with WHERE clauses in each query"""
        sql = """SELECT name FROM client WHERE age > 30
                 UNION
                 SELECT name FROM product WHERE price < 100"""
        sparql = self.converter.convert(sql)

        self.assertIn('UNION', sparql)
        # Each part should have its own WHERE conditions
        # Note: The converter might need enhancement to fully support this

    def test_union_with_order_by(self):
        """Test UNION with ORDER BY (applies to entire result)"""
        sql = """SELECT name FROM client
                 UNION
                 SELECT name FROM product
                 ORDER BY name"""
        sparql = self.converter.convert(sql)

        self.assertIn('UNION', sparql)
        # ORDER BY should apply to the whole query
        self.assertIn('ORDER BY', sparql)

    def test_union_with_limit(self):
        """Test UNION with LIMIT"""
        sql = """SELECT name FROM client
                 UNION
                 SELECT name FROM product
                 LIMIT 10"""
        sparql = self.converter.convert(sql)

        self.assertIn('UNION', sparql)
        self.assertIn('LIMIT 10', sparql)

    def test_union_all_treated_as_union(self):
        """Test UNION ALL (should be treated as UNION in SPARQL)"""
        sql = """SELECT name FROM client
                 UNION ALL
                 SELECT name FROM product"""
        sparql = self.converter.convert(sql)

        # SPARQL doesn't have UNION ALL, should just use UNION
        self.assertIn('UNION', sparql)
        self.assertNotIn('ALL', sparql)

    def test_union_with_different_tables(self):
        """Test UNION across different table structures"""
        sql = """SELECT id, name FROM client
                 UNION
                 SELECT id, name FROM supplier"""
        sparql = self.converter.convert(sql)

        self.assertIn('UNION', sparql)
        # Should handle different source tables correctly

    def test_union_preserves_select_structure(self):
        """Test that UNION preserves the SELECT variable structure"""
        sql = """SELECT name, email FROM client
                 UNION
                 SELECT name, contact FROM supplier"""
        sparql = self.converter.convert(sql)

        # Should use same variables for both parts
        self.assertIn('SELECT ?o0 ?o1', sparql)
        self.assertIn('UNION', sparql)

    def test_union_with_aggregate(self):
        """Test UNION with aggregate functions"""
        sql = """SELECT COUNT(name) FROM client
                 UNION
                 SELECT COUNT(name) FROM product"""
        sparql = self.converter.convert(sql)

        self.assertIn('UNION', sparql)
        # Should handle aggregates in UNION
        self.assertIn('COUNT', sparql)

    def test_union_case_insensitive(self):
        """Test that UNION is case-insensitive"""
        sql = """select name from client
                 union
                 select name from product"""
        sparql = self.converter.convert(sql)

        self.assertIn('UNION', sparql)

    def test_union_with_parentheses(self):
        """Test UNION with parentheses (often used for clarity)"""
        sql = """(SELECT name FROM client)
                 UNION
                 (SELECT name FROM product)"""
        sparql = self.converter.convert(sql)

        self.assertIn('UNION', sparql)
        # Should handle parentheses correctly


if __name__ == '__main__':
    unittest.main()