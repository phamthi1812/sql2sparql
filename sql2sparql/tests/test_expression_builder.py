#!/usr/bin/env python3
"""
Unit tests for ExpressionBuilder class
"""

import unittest
from sql2sparql.core.converter import ExpressionBuilder, ExpressionNode


class TestExpressionBuilder(unittest.TestCase):
    """Test suite for ExpressionBuilder"""

    def setUp(self):
        """Set up test fixtures"""
        self.builder = ExpressionBuilder()
        self.var_mappings = {
            'price': '?price',
            'stock': '?stock',
            'product.price': '?product_price',
            'order.total': '?order_total'
        }

    def test_parse_simple_arithmetic(self):
        """Test parsing simple arithmetic expressions"""
        # Test multiplication
        expr = "price * stock"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'operator')
        self.assertEqual(node.value, '*')
        self.assertEqual(node.left.type, 'operand')
        self.assertEqual(node.left.value, {'column': 'price'})
        self.assertEqual(node.right.type, 'operand')
        self.assertEqual(node.right.value, {'column': 'stock'})

        # Test addition
        expr = "price + 10"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'operator')
        self.assertEqual(node.value, '+')
        self.assertEqual(node.left.value, {'column': 'price'})
        self.assertEqual(node.right.value, '10')

    def test_parse_decimal_numbers(self):
        """Test parsing expressions with decimal numbers"""
        expr = "price * 1.1"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'operator')
        self.assertEqual(node.value, '*')
        self.assertEqual(node.left.value, {'column': 'price'})
        self.assertEqual(node.right.value, '1.1')

    def test_parse_table_column_reference(self):
        """Test parsing table.column references"""
        expr = "product.price * order.quantity"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'operator')
        self.assertEqual(node.value, '*')
        self.assertEqual(node.left.value, {'table': 'product', 'column': 'price'})
        self.assertEqual(node.right.value, {'table': 'order', 'column': 'quantity'})

    def test_parse_parenthesized_expression(self):
        """Test parsing expressions with parentheses"""
        expr = "(price * stock)"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'operator')
        self.assertEqual(node.value, '*')

    def test_parse_complex_expression(self):
        """Test parsing complex expressions with multiple operators"""
        expr = "price * stock + 100"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'operator')
        # Due to left-to-right parsing, + should be root
        self.assertEqual(node.value, '+')

    def test_to_sparql_simple(self):
        """Test converting simple expression to SPARQL"""
        expr = "price * stock"
        node = self.builder.parse_expression(expr)
        sparql = self.builder.to_sparql_expression(node, self.var_mappings)
        self.assertEqual(sparql, "(?price * ?stock)")

    def test_to_sparql_with_literals(self):
        """Test converting expression with literals to SPARQL"""
        expr = "price * 1.1"
        node = self.builder.parse_expression(expr)
        sparql = self.builder.to_sparql_expression(node, self.var_mappings)
        self.assertEqual(sparql, "(?price * 1.1)")

    def test_to_sparql_with_table_columns(self):
        """Test converting expression with table.column to SPARQL"""
        expr = "product.price * 2"
        node = self.builder.parse_expression(expr)
        sparql = self.builder.to_sparql_expression(node, self.var_mappings)
        self.assertEqual(sparql, "(?product_price * 2)")

    def test_parse_function_call(self):
        """Test parsing function calls"""
        expr = "COUNT(price)"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'function')
        self.assertEqual(node.value, 'COUNT')
        self.assertEqual(len(node.arguments), 1)
        self.assertEqual(node.arguments[0].value, {'column': 'price'})

    def test_to_sparql_function(self):
        """Test converting function to SPARQL"""
        expr = "SUM(price)"
        node = self.builder.parse_expression(expr)
        sparql = self.builder.to_sparql_expression(node, self.var_mappings)
        self.assertEqual(sparql, "SUM(?price)")

    def test_parse_subtraction(self):
        """Test parsing subtraction expression"""
        expr = "stock - 5"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'operator')
        self.assertEqual(node.value, '-')
        self.assertEqual(node.left.value, {'column': 'stock'})
        self.assertEqual(node.right.value, '5')

    def test_parse_division(self):
        """Test parsing division expression"""
        expr = "total / quantity"
        node = self.builder.parse_expression(expr)
        self.assertEqual(node.type, 'operator')
        self.assertEqual(node.value, '/')

    def test_mixed_operators(self):
        """Test expression with mixed operators"""
        expr = "price * 1.1 + stock * 2"
        node = self.builder.parse_expression(expr)
        sparql = self.builder.to_sparql_expression(node, self.var_mappings)
        # Should handle operator precedence correctly
        self.assertIn('?price', sparql)
        self.assertIn('?stock', sparql)
        self.assertIn('1.1', sparql)
        self.assertIn('2', sparql)


if __name__ == '__main__':
    unittest.main()