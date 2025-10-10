#!/usr/bin/env python3
"""
Unit tests for complex WHERE clause handling
"""

import unittest
from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.core.schema_mapper import SchemaMapper
from rdflib import Graph


class TestComplexWhere(unittest.TestCase):
    """Test suite for complex WHERE clause conversion"""

    def setUp(self):
        """Set up test fixtures"""
        self.graph = Graph()
        self.schema_mapper = SchemaMapper(self.graph)
        self.converter = SQL2SPARQLConverter(self.schema_mapper)

    def test_simple_and_condition(self):
        """Test WHERE with AND condition"""
        sql = "SELECT name FROM product WHERE category = 'Electronics' AND price < 500"
        sparql = self.converter.convert(sql)

        # Should create separate FILTER statements for AND
        self.assertIn('FILTER(?category = "Electronics")', sparql)
        self.assertIn('FILTER(?price < 500)', sparql)

    def test_simple_or_condition(self):
        """Test WHERE with OR condition"""
        sql = "SELECT name FROM product WHERE category = 'Electronics' OR category = 'Furniture'"
        sparql = self.converter.convert(sql)

        # Should create a single FILTER with || for OR
        self.assertIn('FILTER(?category = "Electronics" || ?category = "Furniture")', sparql)

    def test_mixed_and_or(self):
        """Test WHERE with mixed AND/OR conditions"""
        sql = "SELECT name FROM product WHERE category = 'Electronics' AND price < 500 OR stock > 20"
        sparql = self.converter.convert(sql)

        # Should handle the precedence correctly
        self.assertIn('FILTER(?category = "Electronics")', sparql)
        self.assertIn('FILTER(?price < 500 || ?stock > 20)', sparql)

    def test_like_operator(self):
        """Test LIKE operator conversion"""
        sql = "SELECT name FROM client WHERE email LIKE '%example.com'"
        sparql = self.converter.convert(sql)

        # Should convert to regex
        self.assertIn('regex(?email, ".*example.com", "i")', sparql)

    def test_like_with_underscore(self):
        """Test LIKE operator with underscore wildcard"""
        sql = "SELECT name FROM client WHERE name LIKE 'J_hn'"
        sparql = self.converter.convert(sql)

        # Should convert _ to . in regex
        self.assertIn('regex(?name, "J.hn", "i")', sparql)

    def test_in_operator(self):
        """Test IN operator conversion"""
        sql = "SELECT name FROM product WHERE category IN ('Electronics', 'Furniture')"
        sparql = self.converter.convert(sql)

        # Should convert to SPARQL IN
        self.assertIn('FILTER(?category IN ("Electronics", "Furniture"))', sparql)

    def test_in_with_numbers(self):
        """Test IN operator with numeric values"""
        sql = "SELECT name FROM product WHERE price IN (100, 200, 300)"
        sparql = self.converter.convert(sql)

        # Should handle numeric values without quotes
        self.assertIn('FILTER(?price IN (100, 200, 300))', sparql)

    def test_between_operator(self):
        """Test BETWEEN operator conversion"""
        sql = "SELECT name, price FROM product WHERE price BETWEEN 50 AND 300"
        sparql = self.converter.convert(sql)

        # Should convert to range filter
        self.assertIn('FILTER(?price >= 50 && ?price <= 300)', sparql)

    def test_not_equal_operator(self):
        """Test != and <> operators"""
        sql1 = "SELECT name FROM product WHERE category != 'Electronics'"
        sparql1 = self.converter.convert(sql1)
        self.assertIn('FILTER(?category != "Electronics")', sparql1)

        sql2 = "SELECT name FROM product WHERE category <> 'Electronics'"
        sparql2 = self.converter.convert(sql2)
        self.assertIn('FILTER(?category != "Electronics")', sparql2)

    def test_comparison_operators(self):
        """Test various comparison operators"""
        # Greater than
        sql = "SELECT name FROM product WHERE price > 100"
        sparql = self.converter.convert(sql)
        self.assertIn('FILTER(?price > 100)', sparql)

        # Less than or equal
        sql = "SELECT name FROM product WHERE stock <= 10"
        sparql = self.converter.convert(sql)
        self.assertIn('FILTER(?stock <= 10)', sparql)

        # Greater than or equal
        sql = "SELECT name FROM product WHERE price >= 50"
        sparql = self.converter.convert(sql)
        self.assertIn('FILTER(?price >= 50)', sparql)

    def test_complex_nested_conditions(self):
        """Test complex nested conditions"""
        sql = """SELECT name FROM product
                 WHERE (category = 'Electronics' AND price < 500)
                    OR (category = 'Furniture' AND stock > 10)"""
        sparql = self.converter.convert(sql)

        # Should handle the grouping correctly
        self.assertIn('FILTER', sparql)

    def test_calculated_expression_in_where(self):
        """Test calculated expression in WHERE clause"""
        sql = "SELECT name FROM product WHERE (price * stock) > 1000"
        sparql = self.converter.convert(sql)

        self.assertIn('FILTER((?price * ?stock) > 1000)', sparql)

    def test_multiple_between_clauses(self):
        """Test multiple BETWEEN clauses"""
        sql = "SELECT name FROM product WHERE price BETWEEN 50 AND 100 AND stock BETWEEN 5 AND 20"
        sparql = self.converter.convert(sql)

        # Should create separate filters for each BETWEEN
        self.assertIn('FILTER(?price >= 50 && ?price <= 100)', sparql)
        self.assertIn('FILTER(?stock >= 5 && ?stock <= 20)', sparql)

    def test_table_qualified_columns(self):
        """Test WHERE with table-qualified columns"""
        sql = "SELECT product.name FROM product WHERE product.price > 100"
        sparql = self.converter.convert(sql)

        # Should handle table prefixes - variable name may vary
        self.assertIn('FILTER(?price > 100)', sparql)

    def test_string_literals_with_quotes(self):
        """Test handling of string literals with quotes"""
        sql = "SELECT name FROM product WHERE description = 'High-quality product'"
        sparql = self.converter.convert(sql)

        self.assertIn('FILTER(?description = "High-quality product")', sparql)

    def test_null_comparison(self):
        """Test IS NULL and IS NOT NULL (if supported)"""
        # Note: This might need special handling
        sql = "SELECT name FROM product WHERE description IS NOT NULL"
        # The converter might need enhancement for NULL handling
        sparql = self.converter.convert(sql)
        # Check that it doesn't crash at least
        self.assertIsNotNone(sparql)


if __name__ == '__main__':
    unittest.main()