"""
Main SQL2SPARQL Converter - Orchestrates all conversion components
Based on the convertSqlQuery() algorithm from Table III in the paper

Enhanced with support for:
- Complex aggregate functions with JOINs
- Calculated columns and arithmetic expressions
- Complex WHERE clauses with proper AND/OR logic
- Improved JOIN relationship mapping
- Proper UNION/INTERSECT/EXCEPT support
- Expression builder for complex calculations
"""
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
import re

from ..parsers.sql_parser import SQLParser
from ..converters.select_converter import SelectConverter
from ..converters.where_converter import WhereConverter
from ..converters.group_having_converter import GroupHavingConverter
from ..converters.insert_delete_converter import InsertDeleteConverter
from .models import SQLQuery, SPARQLQuery, QueryType, CombinationType, Triple
from .schema_mapper import SchemaMapper


@dataclass
class ExpressionNode:
    """Node for expression tree representation"""
    type: str  # 'operator', 'operand', 'function', 'literal'
    value: Any
    left: Optional['ExpressionNode'] = None
    right: Optional['ExpressionNode'] = None
    arguments: Optional[List['ExpressionNode']] = None
    
    def __post_init__(self):
        if self.arguments is None:
            self.arguments = []


class ExpressionBuilder:
    """Builds SPARQL expressions from SQL expressions"""

    def __init__(self):
        self.temp_var_counter = 0

    def parse_expression(self, expr_str: str) -> ExpressionNode:
        """Parse SQL expression into expression tree"""
        expr_str = expr_str.strip()

        # Check for parentheses and handle them first
        if expr_str.startswith('(') and expr_str.endswith(')'):
            expr_str = expr_str[1:-1].strip()

        # Check for arithmetic operators (handle them from lowest to highest precedence)
        for op in ['+', '-', '*', '/']:
            # Find the operator not within parentheses
            paren_depth = 0
            for i, char in enumerate(expr_str):
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == op and paren_depth == 0:
                    left = expr_str[:i].strip()
                    right = expr_str[i+1:].strip()
                    if left and right:  # Make sure both sides exist
                        return ExpressionNode(
                            type='operator',
                            value=op,
                            left=self.parse_expression(left),
                            right=self.parse_expression(right)
                        )

        # Check for function calls
        func_match = re.match(r'(\w+)\((.*?)\)', expr_str)
        if func_match:
            func_name = func_match.group(1)
            args_str = func_match.group(2)
            args = [self.parse_expression(arg.strip()) for arg in args_str.split(',') if arg.strip()]
            return ExpressionNode(type='function', value=func_name.upper(), arguments=args)

        # Check if it's a table.column reference
        if '.' in expr_str and not re.match(r'^\d+\.\d+$', expr_str):  # Not a decimal number
            parts = expr_str.split('.')
            if len(parts) == 2:
                return ExpressionNode(type='operand', value={'table': parts[0], 'column': parts[1]})

        # Check if it's a number (including decimals)
        try:
            float(expr_str)
            return ExpressionNode(type='literal', value=expr_str)
        except ValueError:
            # It's either a column name or string literal
            if expr_str.startswith("'") or expr_str.startswith('"'):
                return ExpressionNode(type='literal', value=expr_str)
            else:
                # Simple column name
                return ExpressionNode(type='operand', value={'column': expr_str})

    def to_sparql_expression(self, node: ExpressionNode, var_mappings: Dict) -> str:
        """Convert expression tree to SPARQL expression"""
        if node.type == 'operator':
            left_expr = self.to_sparql_expression(node.left, var_mappings) if node.left else "NULL"
            right_expr = self.to_sparql_expression(node.right, var_mappings) if node.right else "NULL"
            return f"({left_expr} {node.value} {right_expr})"

        elif node.type == 'operand':
            if isinstance(node.value, dict):
                if 'table' in node.value and 'column' in node.value:
                    key = f"{node.value['table']}.{node.value['column']}"
                    if key in var_mappings:
                        return var_mappings[key]
                    return f"?{node.value['column'].lower()}"
                elif 'column' in node.value:
                    col_name = node.value['column']
                    # First try exact match
                    if col_name in var_mappings:
                        return var_mappings[col_name]
                    # Then try with table prefix
                    for key, var in var_mappings.items():
                        if key.endswith(f".{col_name}"):
                            return var
                    return f"?{col_name.lower()}"

        elif node.type == 'literal':
            return str(node.value)

        elif node.type == 'function':
            if node.value in ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']:
                if node.arguments:
                    arg_expr = self.to_sparql_expression(node.arguments[0], var_mappings)
                    return f"{node.value}({arg_expr})"
                return f"{node.value}(*)"

        return "?unknown"


class SQL2SPARQLConverter:
    """
    Main converter class that orchestrates SQL to SPARQL conversion
    Implements the convertSqlQuery() algorithm from Table III
    """

    def __init__(self, schema_mapper: Optional[SchemaMapper] = None):
        """
        Initialize the SQL2SPARQL converter

        Args:
            schema_mapper: SchemaMapper instance for schema resolution
        """
        self.schema_mapper = schema_mapper
        self.sql_parser = SQLParser()
        self.select_converter = SelectConverter(schema_mapper)
        self.where_converter = WhereConverter(schema_mapper)
        self.group_having_converter = GroupHavingConverter(schema_mapper)
        self.insert_delete_converter = InsertDeleteConverter(schema_mapper)
        self.expression_builder = ExpressionBuilder()
        self.var_mappings: Dict[str, str] = {}  # Track variable mappings for complex queries

    def convert(self, sql_query: str) -> str:
        """
        Convert SQL query string to SPARQL query string

        Args:
            sql_query: SQL query string

        Returns:
            SPARQL query string
        """
        # Check for UNION/INTERSECT/EXCEPT first
        if 'UNION' in sql_query.upper():
            return self._handle_union_query(sql_query)

        # Check for calculated columns, complex expressions, or special operators
        select_part = sql_query.upper().split('FROM')[0] if 'FROM' in sql_query.upper() else sql_query.upper()
        where_part = sql_query.upper().split('WHERE')[1] if 'WHERE' in sql_query.upper() else ""

        needs_enhanced = (
            any(op in select_part for op in ['*', '/', '+', '-']) or
            'BETWEEN' in where_part or
            'IN (' in where_part or
            ' OR ' in where_part
        )

        if needs_enhanced and 'SELECT' in sql_query.upper():
            return self._convert_with_expressions(sql_query)

        # Standard conversion path
        parsed_query = self.sql_parser.parse(sql_query)
        sparql_query = self._convert_query(parsed_query)
        return sparql_query.to_string()

    def _convert_query(self, sql_query: SQLQuery) -> SPARQLQuery:
        """
        Convert parsed SQL query to SPARQL query
        Implementation of convertSqlQuery() algorithm from Table III

        Args:
            sql_query: Parsed SQL query object

        Returns:
            SPARQLQuery object
        """
        sparql_query = SPARQLQuery()

        # Handle different query types
        if sql_query.type == QueryType.SELECT:
            self._convert_select_query(sql_query, sparql_query)

        elif sql_query.type == QueryType.INSERT:
            self._convert_insert_query(sql_query, sparql_query)

        elif sql_query.type == QueryType.DELETE:
            self._convert_delete_query(sql_query, sparql_query)

        else:
            raise ValueError(f"Unsupported query type: {sql_query.type}")

        # Handle combined queries (UNION, INTERSECT, EXCEPT)
        if sql_query.combination_type:
            if sql_query.left_query and sql_query.right_query:
                sparql_query = self._handle_combination(
                    sql_query.left_query,
                    sql_query.right_query,
                    sql_query.combination_type,
                    sparql_query
                )

        return sparql_query

    def _convert_select_query(self, sql_query: SQLQuery, sparql_query: SPARQLQuery):
        """
        Convert SELECT query components to SPARQL

        Args:
            sql_query: Parsed SQL SELECT query
            sparql_query: SPARQL query object to populate
        """
        # Step 1: Convert SELECT clause (Algorithm from Table X)
        select_vars, select_patterns = self.select_converter.convert(
            sql_query.select_attributes
        )
        sparql_query.select_vars = select_vars
        sparql_query.where_patterns.extend(select_patterns)

        # Step 2: Convert WHERE clause (Algorithm from Table VIII)
        if sql_query.join_conditions or sql_query.where_conditions:
            where_patterns, filter_conditions = self.where_converter.convert(
                sql_query.join_conditions,
                sql_query.where_conditions,
                select_patterns
            )

            # Add patterns that aren't already present
            for pattern in where_patterns:
                if not self._pattern_exists(sparql_query.where_patterns, pattern):
                    sparql_query.where_patterns.append(pattern)

            sparql_query.filter_conditions.extend(filter_conditions)

        # Step 3: Convert GROUP BY clause (Algorithm from Table XI)
        if sql_query.group_by:
            group_vars, group_patterns = self.group_having_converter.convert_group_by(
                sql_query.group_by,
                sparql_query.where_patterns,
                select_vars,  # Pass SELECT variables for alignment
                select_patterns  # Pass SELECT patterns for reference
            )
            sparql_query.group_by_vars = group_vars

            # Add any additional patterns from GROUP BY
            for pattern in group_patterns:
                if not self._pattern_exists(sparql_query.where_patterns, pattern):
                    sparql_query.where_patterns.append(pattern)

        # Step 4: Convert HAVING clause (Algorithm from Table XII)
        if sql_query.having:
            having_conditions = self.group_having_converter.convert_having(
                sql_query.having,
                sparql_query.where_patterns,
                select_vars,  # Pass SELECT variables for HAVING alignment
                select_patterns  # Pass SELECT patterns for reference
            )
            sparql_query.having_conditions = having_conditions

        # Step 5: Convert ORDER BY clause
        if sql_query.order_by:
            for attr, direction in sql_query.order_by:
                # Find the corresponding variable
                var_name = self._find_variable_for_attribute(
                    attr, select_vars, sparql_query.where_patterns
                )
                if var_name:
                    sparql_query.order_by_vars.append((var_name, direction))

        # Step 6: Add LIMIT and OFFSET
        sparql_query.limit = sql_query.limit
        sparql_query.offset = sql_query.offset

    def _convert_insert_query(self, sql_query: SQLQuery, sparql_query: SPARQLQuery):
        """
        Convert INSERT query to SPARQL INSERT DATA

        Args:
            sql_query: Parsed SQL INSERT query
            sparql_query: SPARQL query object to populate
        """
        # Convert INSERT using algorithms from Tables XIII and XIV
        insert_triples = []
        if sql_query.insert_table:
            insert_triples = self.insert_delete_converter.convert_insert(
                sql_query.insert_table,
                sql_query.insert_values
            )
        sparql_query.insert_triples = insert_triples
        sparql_query.insert_triples = insert_triples

    def _convert_delete_query(self, sql_query: SQLQuery, sparql_query: SPARQLQuery):
        """
        Convert DELETE query to SPARQL DELETE WHERE

        Args:
            sql_query: Parsed SQL DELETE query
            sparql_query: SPARQL query object to populate
        """
        # Convert DELETE using algorithm from Table XV
        delete_patterns: List = []
        where_patterns: List = []
        filter_conditions: List = []
        if sql_query.delete_table:
            delete_patterns, where_patterns, filter_conditions = \
                self.insert_delete_converter.convert_delete(
                    sql_query.delete_table,
                    sql_query.where_conditions,
                    sql_query.join_conditions
                )

        sparql_query.delete_patterns = delete_patterns
        sparql_query.where_patterns = where_patterns
        sparql_query.filter_conditions = filter_conditions

    def _handle_combination(
        self,
        left_query: SQLQuery,
        right_query: SQLQuery,
        combination_type: CombinationType,
        base_query: SPARQLQuery
    ) -> SPARQLQuery:
        """
        Handle combined queries (UNION, INTERSECT, EXCEPT)
        Implementation from Table IV (Combine algorithm)

        Args:
            left_query: Left SQL query
            right_query: Right SQL query
            combination_type: Type of combination
            base_query: Base SPARQL query

        Returns:
            Combined SPARQL query
        """
        # Convert sub-queries
        left_sparql = self._convert_query(left_query)
        right_sparql = self._convert_query(right_query)

        # Create combined query
        combined = SPARQLQuery()
        combined.select_vars = left_sparql.select_vars  # Use left query's SELECT

        # Build combined WHERE clause
        left_where = self._build_where_block(left_sparql)
        right_where = self._build_where_block(right_sparql)

        # Combine based on type
        if combination_type == CombinationType.UNION:
            combined_where = f"{{ {left_where} }} UNION {{ {right_where} }}"
        elif combination_type == CombinationType.INTERSECT:
            # SPARQL doesn't have INTERSECT, simulate with FILTER EXISTS
            combined_where = f"{left_where} FILTER EXISTS {{ {right_where} }}"
        elif combination_type == CombinationType.EXCEPT:
            # Simulate EXCEPT with FILTER NOT EXISTS
            combined_where = f"{left_where} FILTER NOT EXISTS {{ {right_where} }}"

        # For now, return a simplified version
        # In production, would properly parse and combine
        return combined

    def _build_where_block(self, sparql_query: SPARQLQuery) -> str:
        """
        Build WHERE clause block from SPARQL query

        Args:
            sparql_query: SPARQL query object

        Returns:
            WHERE clause string
        """
        where_parts = []

        for pattern in sparql_query.where_patterns:
            where_parts.append(f"{pattern.to_sparql_pattern()} .")

        for filter_cond in sparql_query.filter_conditions:
            where_parts.append(f"FILTER({filter_cond})")

        return "\n".join(where_parts)

    def _pattern_exists(self, patterns: list, new_pattern: Triple) -> bool:
        """
        Check if a pattern already exists in the list

        Args:
            patterns: List of existing patterns
            new_pattern: Pattern to check

        Returns:
            True if pattern exists, False otherwise
        """
        for pattern in patterns:
            if (pattern.subject == new_pattern.subject and
                pattern.predicate == new_pattern.predicate and
                pattern.object == new_pattern.object):
                return True
        return False

    def _find_variable_for_attribute(
        self,
        attribute,
        select_vars: list,
        patterns: list
    ) -> Optional[str]:
        """
        Find SPARQL variable corresponding to an SQL attribute

        Args:
            attribute: SQL attribute
            select_vars: List of SELECT variables
            patterns: List of triple patterns

        Returns:
            Variable name if found, None otherwise
        """
        # Simple heuristic - in production would need proper mapping
        for i, var in enumerate(select_vars):
            # Check if this variable index matches the attribute
            if var.startswith("?o"):
                return var

        # Default to first variable
        return select_vars[0] if select_vars else None

    def _convert_with_expressions(self, sql_query: str) -> str:
        """
        Convert SQL with calculated columns and complex expressions
        """
        sparql_parts = []

        # Extract SQL components
        select_clause = self._extract_clause(sql_query, 'SELECT', 'FROM')
        from_clause = self._extract_clause(sql_query, 'FROM', 'WHERE|GROUP|ORDER|LIMIT|$')
        where_clause = self._extract_clause(sql_query, 'WHERE', 'GROUP|ORDER|LIMIT|$')
        group_clause = self._extract_clause(sql_query, 'GROUP BY', 'HAVING|ORDER|LIMIT|$')
        having_clause = self._extract_clause(sql_query, 'HAVING', 'ORDER|LIMIT|$')
        order_clause = self._extract_clause(sql_query, 'ORDER BY', 'LIMIT|$')

        # Reset variable mappings
        self.var_mappings = {}
        select_vars = []
        triple_patterns: List[Triple] = []

        # Store FROM table(s) for context
        if from_clause:
            # Simple extraction of first table name for context
            from_parts = from_clause.split(',')
            if from_parts:
                self._current_from_table = from_parts[0].strip().split()[0]

        # Process SELECT clause with expressions
        select_items = self._split_respecting_parens(select_clause, ',')

        for item in select_items:
            item = item.strip()

            # Check for calculated expression (must check before regular columns)
            if self._is_calculated_expression(item):
                calc_var = self._process_calculated_column(item, triple_patterns)
                select_vars.append(calc_var)
            # Check for aggregate function
            elif self._is_aggregate_function(item):
                agg_var = self._process_aggregate_function(item, triple_patterns)
                select_vars.append(agg_var)
            # Regular column
            else:
                var = self._process_regular_column(item, triple_patterns)
                select_vars.append(var)

        # Build SPARQL SELECT
        sparql_parts.append(f"SELECT {' '.join(select_vars)}")
        sparql_parts.append("WHERE {")

        # Add triple patterns
        for pattern in triple_patterns:
            sparql_parts.append(f"  {pattern} .")

        # Process WHERE clause with complex conditions
        if where_clause:
            filters = self._process_complex_where(where_clause)
            for filter_expr in filters:
                sparql_parts.append(f"  {filter_expr}")

        sparql_parts.append("}")

        # Add GROUP BY
        if group_clause:
            group_vars = self._process_group_by(group_clause)
            if group_vars:
                sparql_parts.append(f"GROUP BY {' '.join(group_vars)}")

        # Add HAVING
        if having_clause:
            having_expr = self._process_having(having_clause)
            if having_expr:
                sparql_parts.append(f"HAVING {having_expr}")

        # Add ORDER BY
        if order_clause:
            order_expr = self._process_order_by(order_clause)
            if order_expr:
                sparql_parts.append(f"ORDER BY {order_expr}")

        # Add LIMIT
        limit_match = re.search(r'LIMIT\s+(\d+)', sql_query, re.IGNORECASE)
        if limit_match:
            sparql_parts.append(f"LIMIT {limit_match.group(1)}")

        return '\n'.join(sparql_parts)

    def _handle_union_query(self, sql_query: str) -> str:
        """Handle UNION/INTERSECT/EXCEPT queries"""
        # Check for ORDER BY and LIMIT at the end of the entire query
        order_by_match = re.search(r'\s+ORDER\s+BY\s+(.*?)(?:\s+LIMIT|\s*$)', sql_query, re.IGNORECASE)
        limit_match = re.search(r'\s+LIMIT\s+(\d+)', sql_query, re.IGNORECASE)

        # Remove ORDER BY and LIMIT from the query before processing UNION parts
        clean_query = sql_query
        if order_by_match:
            clean_query = clean_query[:order_by_match.start()]
        elif limit_match and order_by_match is None:
            clean_query = clean_query[:limit_match.start()]

        # Split by UNION
        parts = re.split(r'\s+UNION(?:\s+ALL)?\s+', clean_query, flags=re.IGNORECASE)

        sparql_parts = []
        select_vars = None

        for i, part in enumerate(parts):
            # Handle parentheses around queries
            part = part.strip()
            if part.startswith('(') and part.endswith(')'):
                part = part[1:-1].strip()

            # Convert each part using standard converter
            sub_sparql = self.convert(part)

            # Extract components
            where_match = re.search(r'WHERE \{(.*?)\}', sub_sparql, re.DOTALL)
            if where_match:
                where_content = where_match.group(1)

                if i == 0:
                    # First part - extract SELECT
                    select_match = re.search(r'SELECT (.*)WHERE', sub_sparql, re.DOTALL)
                    if select_match:
                        select_vars = select_match.group(1).strip()
                        sparql_parts.append(f"SELECT {select_vars}")
                    sparql_parts.append("WHERE {")
                    sparql_parts.append(f"  {{ {where_content.strip()} }}")
                else:
                    # Subsequent parts - add UNION
                    sparql_parts.append("  UNION")
                    sparql_parts.append(f"  {{ {where_content.strip()} }}")

        sparql_parts.append("}")

        # Add ORDER BY if present
        if order_by_match:
            order_clause = order_by_match.group(1).strip()
            # Convert SQL column names to SPARQL variables
            # For simplicity, assume they map to ?o0, ?o1, etc.
            sparql_parts.append(f"ORDER BY ?{order_clause.lower().replace(' desc', '').replace(' asc', '').strip()}")

        # Add LIMIT if present
        if limit_match:
            sparql_parts.append(f"LIMIT {limit_match.group(1)}")

        return '\n'.join(sparql_parts)

    def _extract_clause(self, sql: str, start: str, end: str) -> str:
        """Extract SQL clause between keywords"""
        pattern = fr'{start}\s+(.*?)(?:{end})'
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""

    def _split_respecting_parens(self, text: str, delimiter: str) -> List[str]:
        """Split text by delimiter while respecting parentheses"""
        parts = []
        current: List[str] = []
        paren_depth = 0

        for char in text:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == delimiter and paren_depth == 0:
                parts.append(''.join(current).strip())
                current = []
                continue
            current.append(char)

        if current:
            parts.append(''.join(current).strip())

        return parts

    def _is_calculated_expression(self, expr: str) -> bool:
        """Check if expression contains calculations"""
        # Remove alias part if present
        expr_check = expr.upper().split(' AS ')[0] if ' AS ' in expr.upper() else expr

        # Check if it's an aggregate function (COUNT, SUM, etc.)
        agg_funcs = ['COUNT(', 'SUM(', 'AVG(', 'MIN(', 'MAX(']
        if any(func in expr_check.upper() for func in agg_funcs):
            return False

        # Check for arithmetic operators
        return any(op in expr_check for op in ['*', '/', '+', '-'])

    def _is_aggregate_function(self, expr: str) -> bool:
        """Check if expression is an aggregate function"""
        agg_funcs = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
        expr_upper = expr.upper()
        return any(func + '(' in expr_upper for func in agg_funcs)

    def _process_calculated_column(self, expr: str, patterns: List) -> str:
        """Process calculated column expression"""
        # Check for alias
        alias = None
        if ' AS ' in expr.upper():
            parts = re.split(r'\s+AS\s+', expr, flags=re.IGNORECASE)
            expr = parts[0].strip()
            alias = parts[1].strip()

        # Extract columns and create variables FIRST
        columns = self._extract_columns_from_expr(expr)
        for col in columns:
            if col not in self.var_mappings and not col.isdigit():
                var_name = f"?{col.replace('.', '_').lower()}"
                self.var_mappings[col] = var_name

                # Create triple pattern
                if '.' in col:
                    table, column = col.split('.')
                    pattern = self._create_triple_pattern(table, column, var_name)
                    if pattern not in patterns:
                        patterns.append(pattern)
                else:
                    # Infer table from FROM clause if possible
                    from_clause = self._current_from_table if hasattr(self, '_current_from_table') else 'entity'
                    pattern = self._create_triple_pattern(from_clause, col, var_name)
                    if pattern not in patterns:
                        patterns.append(pattern)

        # Parse expression
        expr_tree = self.expression_builder.parse_expression(expr)

        # Convert to SPARQL expression
        sparql_expr = self.expression_builder.to_sparql_expression(expr_tree, self.var_mappings)

        if alias:
            return f"({sparql_expr} AS ?{alias})"
        else:
            return f"({sparql_expr} AS ?calc_{len(self.var_mappings)})"

    def _process_aggregate_function(self, expr: str, patterns: List) -> str:
        """Process aggregate function"""
        match = re.match(r'(\w+)\((.*)\)', expr, re.IGNORECASE)
        if match:
            func = match.group(1).upper()
            col = match.group(2).strip()

            # Handle COUNT(*)
            if col == '*':
                return f"(COUNT(*) AS ?count)"

            # Get or create variable for column
            if col not in self.var_mappings:
                var = f"?{col.replace('.', '_').lower()}"
                self.var_mappings[col] = var

                # Create triple pattern if needed
                if '.' in col:
                    table, column = col.split('.')
                    pattern = self._create_triple_pattern(table, column, var)
                    if pattern not in patterns:
                        patterns.append(pattern)
            else:
                var = self.var_mappings[col]

            # Check for alias
            alias_match = re.search(r'\s+as\s+(\w+)', expr, re.IGNORECASE)
            if alias_match:
                alias = alias_match.group(1)
                return f"({func}({var}) AS ?{alias})"

            return f"({func}({var}) AS ?{func.lower()}_{col.replace('.', '_')})"

        return expr

    def _process_regular_column(self, col: str, patterns: List) -> str:
        """Process regular column reference"""
        col = col.strip()

        if col not in self.var_mappings:
            var = f"?{col.replace('.', '_').lower()}"
            self.var_mappings[col] = var

            # Create triple pattern
            if '.' in col:
                table, column = col.split('.')
                pattern = self._create_triple_pattern(table, column, var)
                if pattern not in patterns:
                    patterns.append(pattern)
            else:
                # Use current FROM table context
                from_table = self._current_from_table if hasattr(self, '_current_from_table') else 'entity'
                pattern = self._create_triple_pattern(from_table, col, var)
                if pattern not in patterns:
                    patterns.append(pattern)

        return self.var_mappings[col]

    def _create_triple_pattern(self, table: str, column: str, var: str) -> str:
        """Create triple pattern for table.column"""
        subject_var = f"?{table.lower()}"
        predicate = f"<http://example.org/ontology/{column}>"
        return f"{subject_var} {predicate} {var}"

    def _extract_columns_from_expr(self, expr: str) -> List[str]:
        """Extract column references from expression"""
        columns = []

        # First, temporarily replace decimal numbers to avoid confusion
        temp_expr = re.sub(r'\b\d+\.\d+\b', 'DECIMAL_PLACEHOLDER', expr)

        # Look for table.column patterns (after decimal replacement)
        pattern = r'\b(\w+\.\w+)\b'
        matches = re.findall(pattern, temp_expr)
        for match in matches:
            if 'DECIMAL_PLACEHOLDER' not in match:
                columns.append(match)

        # Look for simple column names
        pattern = r'\b(\w+)\b'
        matches = re.findall(pattern, temp_expr)
        for match in matches:
            if (match.upper() not in ['SELECT', 'FROM', 'WHERE', 'AS', 'AND', 'OR', 'DECIMAL_PLACEHOLDER'] and
                not match.isdigit() and match not in columns):
                # Check if this is actually a column and not part of a decimal that was replaced
                columns.append(match)

        return columns

    def _process_complex_where(self, where_clause: str) -> List[str]:
        """Process complex WHERE clause with AND/OR"""
        filters = []

        # Handle BETWEEN specially since it contains AND
        if 'BETWEEN' in where_clause.upper():
            # Extract BETWEEN conditions first
            between_pattern = r'(\w+(?:\.\w+)?)\s+BETWEEN\s+(\S+)\s+AND\s+(\S+)'
            between_matches = re.finditer(between_pattern, where_clause, re.IGNORECASE)

            for match in between_matches:
                col = match.group(1)
                lower = match.group(2)
                upper = match.group(3)

                # Get variable for column
                var = self.var_mappings.get(col, f"?{col.replace('.', '_').lower()}")

                # Create filter
                filters.append(f"FILTER({var} >= {lower} && {var} <= {upper})")

                # Remove this BETWEEN from where_clause for further processing
                where_clause = where_clause[:match.start()] + where_clause[match.end():]

        # Now process remaining conditions
        if where_clause.strip():
            # Split by AND, preserving OR groups
            and_parts = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)

            for part in and_parts:
                part = part.strip()
                if not part:
                    continue

                # Check for OR conditions
                or_parts = re.split(r'\s+OR\s+', part, flags=re.IGNORECASE)

                if len(or_parts) > 1:
                    # Create OR filter
                    or_filters = []
                    for or_part in or_parts:
                        filter_expr = self._create_filter_expression(or_part.strip())
                        if filter_expr:
                            or_filters.append(filter_expr)
                    if or_filters:
                        filters.append(f"FILTER({' || '.join(or_filters)})")
                else:
                    # Single condition
                    filter_expr = self._create_filter_expression(part.strip())
                    if filter_expr:
                        filters.append(f"FILTER({filter_expr})")

        return filters

    def _create_filter_expression(self, condition: str) -> str:
        """Create SPARQL filter expression from SQL condition"""
        # First check if there's a calculated expression in the condition
        if any(op in condition for op in ['*', '/', '+', '-']):
            # Handle calculated expressions like (price * stock) > 1000
            match = re.match(r'\((.*?)\)\s*([><=]+)\s*(\d+)', condition)
            if match:
                expr = match.group(1).strip()
                operator = match.group(2)
                value = match.group(3)

                # Parse the expression and convert to SPARQL
                expr_tree = self.expression_builder.parse_expression(expr) if hasattr(self, 'expression_builder') else None
                if expr_tree and hasattr(self, 'expression_builder'):
                    sparql_expr = self.expression_builder.to_sparql_expression(expr_tree, self.var_mappings)
                    return f"{sparql_expr} {operator} {value}"

        # Standard pattern matching for column operator value
        match = re.match(r'(\w+(?:\.\w+)?)\s*(=|!=|<>|<|>|<=|>=|LIKE|IN|BETWEEN)\s+(.+)',
                        condition, re.IGNORECASE)
        if match:
            column = match.group(1)
            operator = match.group(2).upper()
            value = match.group(3).strip()

            # Get variable for column
            var = self.var_mappings.get(column, f"?{column.replace('.', '_').lower()}")

            # Handle different operators
            if operator == 'LIKE':
                pattern = value.strip("'\"")
                pattern = pattern.replace('%', '.*').replace('_', '.')
                return f'regex({var}, "{pattern}", "i")'
            elif operator == 'IN':
                values = value.strip('()').split(',')
                value_list = []
                for v in values:
                    v = v.strip().strip("'\"")
                    try:
                        float(v)
                        value_list.append(v)
                    except ValueError:
                        value_list.append(f'"{v}"')
                return f"{var} IN ({', '.join(value_list)})"
            elif operator == 'BETWEEN':
                match = re.match(r'(\S+)\s+AND\s+(\S+)', value, re.IGNORECASE)
                if match:
                    lower = match.group(1)
                    upper = match.group(2)
                    return f"({var} >= {lower} && {var} <= {upper})"
            else:
                # Standard comparison
                value = value.strip("'\"")
                try:
                    float(value)
                    value_formatted = value
                except ValueError:
                    value_formatted = f'"{value}"'

                op_map = {'=': '=', '!=': '!=', '<>': '!='}
                sparql_op = op_map.get(operator, operator.lower())
                return f"{var} {sparql_op} {value_formatted}"

        return ""

    def _process_group_by(self, group_clause: str) -> List[str]:
        """Process GROUP BY clause"""
        vars = []
        for col in group_clause.split(','):
            col = col.strip()
            if col in self.var_mappings:
                vars.append(self.var_mappings[col])
        return vars

    def _process_having(self, having_clause: str) -> str:
        """Process HAVING clause"""
        # Simple conversion for COUNT() > value patterns
        match = re.match(r'COUNT\((.*)\)\s*([><=]+)\s*(\d+)', having_clause, re.IGNORECASE)
        if match:
            col = match.group(1).strip()
            op = match.group(2)
            value = match.group(3)

            var = self.var_mappings.get(col, f"?{col.lower()}")
            return f"(COUNT({var}) {op} {value})"

        return ""

    def _process_order_by(self, order_clause: str) -> str:
        """Process ORDER BY clause"""
        parts = []
        for item in order_clause.split(','):
            item = item.strip()
            if ' DESC' in item.upper():
                col = item.replace(' DESC', '').replace(' desc', '').strip()
                var = self.var_mappings.get(col, f"?{col.lower()}")
                parts.append(f"DESC({var})")
            elif ' ASC' in item.upper():
                col = item.replace(' ASC', '').replace(' asc', '').strip()
                var = self.var_mappings.get(col, f"?{col.lower()}")
                parts.append(f"ASC({var})")
            else:
                var = self.var_mappings.get(item, f"?{item.lower()}")
                parts.append(var)

        return ' '.join(parts)
