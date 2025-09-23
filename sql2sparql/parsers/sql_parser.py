"""
SQL Parser - Parses SQL queries and extracts components for conversion
"""
import re
import sqlparse
from typing import List, Dict, Any, Optional, Tuple
from sqlparse.sql import Token, TokenList, Identifier, IdentifierList, Where, Comparison, Function
from sqlparse.tokens import Keyword, DML

from ..core.models import (
    SQLQuery, QueryType, Attribute, JoinCondition,
    WhereCondition, AggregateFunction, CombinationType
)


class SQLParser:
    """
    Parses SQL queries and extracts components needed for SPARQL conversion
    """

    def __init__(self):
        """Initialize SQL parser"""
        self.aggregate_map = {
            'COUNT': AggregateFunction.COUNT,
            'SUM': AggregateFunction.SUM,
            'AVG': AggregateFunction.AVG,
            'MIN': AggregateFunction.MIN,
            'MAX': AggregateFunction.MAX
        }

    def parse(self, sql_query: str) -> SQLQuery:
        """
        Parse SQL query string into SQLQuery object

        Args:
            sql_query: SQL query string

        Returns:
            SQLQuery object with parsed components
        """
        # Clean and format the query
        sql_query = sql_query.strip()
        if not sql_query.endswith(';'):
            sql_query += ';'

        # Parse using sqlparse
        parsed = sqlparse.parse(sql_query)[0]

        # Determine query type
        query_type = self._get_query_type(parsed)

        # Create SQLQuery object
        query = SQLQuery(type=query_type)

        # Parse based on query type
        if query_type == QueryType.SELECT:
            self._parse_select_query(parsed, query)
        elif query_type == QueryType.INSERT:
            self._parse_insert_query(parsed, query)
        elif query_type == QueryType.DELETE:
            self._parse_delete_query(parsed, query)
        else:
            raise ValueError(f"Unsupported query type: {query_type}")

        return query

    def _get_query_type(self, parsed: TokenList) -> QueryType:
        """Determine the type of SQL query"""
        for token in parsed.tokens:
            if token.ttype is DML:
                keyword = token.value.upper()
                if keyword == 'SELECT':
                    return QueryType.SELECT
                elif keyword == 'INSERT':
                    return QueryType.INSERT
                elif keyword == 'DELETE':
                    return QueryType.DELETE
                elif keyword == 'UPDATE':
                    return QueryType.UPDATE
        raise ValueError("Unable to determine query type")

    def _parse_select_query(self, parsed: TokenList, query: SQLQuery):
        """Parse SELECT query components"""
        idx = 0
        tokens = list(parsed.flatten())

        while idx < len(tokens):
            token = tokens[idx]

            # Skip whitespace and punctuation
            if token.is_whitespace or str(token) in [',', ';', '(', ')']:
                idx += 1
                continue

            keyword = str(token).upper()

            if keyword == 'SELECT':
                idx = self._parse_select_clause(tokens, idx + 1, query)
            elif keyword == 'FROM':
                idx = self._parse_from_clause(tokens, idx + 1, query)
            elif keyword == 'WHERE':
                idx = self._parse_where_clause(tokens, idx + 1, query)
            elif keyword == 'GROUP' and idx + 1 < len(tokens) and str(tokens[idx + 1]).upper() == 'BY':
                idx = self._parse_group_by_clause(tokens, idx + 2, query)
            elif keyword == 'HAVING':
                idx = self._parse_having_clause(tokens, idx + 1, query)
            elif keyword == 'ORDER' and idx + 1 < len(tokens) and str(tokens[idx + 1]).upper() == 'BY':
                idx = self._parse_order_by_clause(tokens, idx + 2, query)
            elif keyword == 'LIMIT':
                idx = self._parse_limit_clause(tokens, idx + 1, query)
            elif keyword == 'OFFSET':
                idx = self._parse_offset_clause(tokens, idx + 1, query)
            elif keyword in ['UNION', 'INTERSECT', 'EXCEPT']:
                # Handle combined queries
                query.combination_type = CombinationType[keyword]
                # For simplicity, we'll handle this in a separate method
                idx += 1
            else:
                idx += 1

    def _parse_select_clause(self, tokens: List, start_idx: int, query: SQLQuery) -> int:
        """Parse SELECT clause and extract attributes"""
        idx = start_idx
        current_attr = []

        while idx < len(tokens):
            token = tokens[idx]
            token_str = str(token).strip()

            # Check for end of SELECT clause
            if token_str.upper() in ['FROM', 'WHERE', 'GROUP', 'ORDER', 'LIMIT', ';']:
                if current_attr:
                    self._add_attribute(current_attr, query)
                return idx

            # Handle comma separator
            if token_str == ',':
                if current_attr:
                    self._add_attribute(current_attr, query)
                    current_attr = []
            elif not token.is_whitespace:
                current_attr.append(token_str)

            idx += 1

        if current_attr:
            self._add_attribute(current_attr, query)

        return idx

    def _add_attribute(self, attr_parts: List[str], query: SQLQuery):
        """Add parsed attribute to query"""
        attr_str = ' '.join(attr_parts)

        # Check for aggregate function
        aggregate = None
        for agg_name, agg_enum in self.aggregate_map.items():
            if attr_str.upper().startswith(agg_name):
                aggregate = agg_enum
                # Extract attribute from aggregate function
                match = re.match(rf'{agg_name}\s*\(\s*([^)]+)\s*\)', attr_str, re.IGNORECASE)
                if match:
                    attr_str = match.group(1)
                break

        # Parse table.attribute format
        if '.' in attr_str:
            parts = attr_str.split('.')
            relation = parts[0].strip()
            name = parts[1].strip()
        else:
            # Single attribute without table prefix
            relation = ""
            name = attr_str.strip()

        # Handle alias (AS keyword)
        alias = None
        if ' AS ' in name.upper():
            parts = re.split(r'\s+AS\s+', name, flags=re.IGNORECASE)
            name = parts[0].strip()
            alias = parts[1].strip()

        attribute = Attribute(
            relation=relation,
            name=name,
            alias=alias,
            aggregate=aggregate
        )
        query.select_attributes.append(attribute)

    def _parse_from_clause(self, tokens: List, start_idx: int, query: SQLQuery) -> int:
        """Parse FROM clause and extract tables"""
        idx = start_idx
        current_table = []

        while idx < len(tokens):
            token = tokens[idx]
            token_str = str(token).strip()

            # Check for end of FROM clause
            if token_str.upper() in ['WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', ';']:
                if current_table:
                    table_name = ' '.join(current_table).strip()
                    if table_name:
                        query.from_tables.append(table_name)
                return idx

            # Handle comma separator
            if token_str == ',':
                if current_table:
                    table_name = ' '.join(current_table).strip()
                    if table_name:
                        query.from_tables.append(table_name)
                    current_table = []
            elif not token.is_whitespace:
                current_table.append(token_str)

            idx += 1

        if current_table:
            table_name = ' '.join(current_table).strip()
            if table_name:
                query.from_tables.append(table_name)

        return idx

    def _parse_where_clause(self, tokens: List, start_idx: int, query: SQLQuery) -> int:
        """Parse WHERE clause and extract conditions"""
        idx = start_idx
        condition_parts = []

        while idx < len(tokens):
            token = tokens[idx]
            token_str = str(token).strip()

            # Check for end of WHERE clause
            if token_str.upper() in ['GROUP', 'HAVING', 'ORDER', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT', ';']:
                if condition_parts:
                    self._parse_conditions(' '.join(condition_parts), query)
                return idx

            if not token.is_whitespace:
                condition_parts.append(token_str)

            idx += 1

        if condition_parts:
            self._parse_conditions(' '.join(condition_parts), query)

        return idx

    def _parse_conditions(self, condition_str: str, query: SQLQuery):
        """Parse WHERE conditions and separate joins from filters"""
        # Split by AND (simplified - real implementation would need proper parsing)
        conditions = re.split(r'\s+AND\s+', condition_str, flags=re.IGNORECASE)

        for cond in conditions:
            cond = cond.strip()
            if not cond:
                continue

            # Check if it's a join condition (table1.attr = table2.attr)
            join_match = re.match(
                r'(\w+)\.(\w+)\s*(=)\s*(\w+)\.(\w+)',
                cond,
                re.IGNORECASE
            )

            if join_match:
                # This is a join condition
                left_table = join_match.group(1)
                left_attr = join_match.group(2)
                right_table = join_match.group(4)
                right_attr = join_match.group(5)

                join_cond = JoinCondition(
                    left_operand=Attribute(relation=left_table, name=left_attr),
                    right_operand=Attribute(relation=right_table, name=right_attr),
                    operator='='
                )
                query.join_conditions.append(join_cond)
            else:
                # This is a regular WHERE condition
                # Parse comparison: attribute operator value
                comp_match = re.match(
                    r'([\w.]+)\s*([=<>!]+)\s*(.+)',
                    cond,
                    re.IGNORECASE
                )

                if comp_match:
                    attr_str = comp_match.group(1)
                    operator = comp_match.group(2)
                    value = comp_match.group(3).strip().strip("'\"")

                    # Parse attribute
                    if '.' in attr_str:
                        parts = attr_str.split('.')
                        relation = parts[0]
                        name = parts[1]
                    else:
                        relation = ""
                        name = attr_str

                    where_cond = WhereCondition(
                        attribute=Attribute(relation=relation, name=name),
                        operator=operator,
                        value=value,
                        is_join=False
                    )
                    query.where_conditions.append(where_cond)

    def _parse_group_by_clause(self, tokens: List, start_idx: int, query: SQLQuery) -> int:
        """Parse GROUP BY clause"""
        idx = start_idx
        current_attr = []

        while idx < len(tokens):
            token = tokens[idx]
            token_str = str(token).strip()

            # Check for end of GROUP BY clause
            if token_str.upper() in ['HAVING', 'ORDER', 'LIMIT', ';']:
                if current_attr:
                    self._add_group_by_attribute(current_attr, query)
                return idx

            # Handle comma separator
            if token_str == ',':
                if current_attr:
                    self._add_group_by_attribute(current_attr, query)
                    current_attr = []
            elif not token.is_whitespace:
                current_attr.append(token_str)

            idx += 1

        if current_attr:
            self._add_group_by_attribute(current_attr, query)

        return idx

    def _add_group_by_attribute(self, attr_parts: List[str], query: SQLQuery):
        """Add GROUP BY attribute to query"""
        attr_str = ' '.join(attr_parts)

        if '.' in attr_str:
            parts = attr_str.split('.')
            relation = parts[0].strip()
            name = parts[1].strip()
        else:
            relation = ""
            name = attr_str.strip()

        attribute = Attribute(relation=relation, name=name)
        query.group_by.append(attribute)

    def _parse_having_clause(self, tokens: List, start_idx: int, query: SQLQuery) -> int:
        """Parse HAVING clause"""
        idx = start_idx
        condition_parts = []

        while idx < len(tokens):
            token = tokens[idx]
            token_str = str(token).strip()

            # Check for end of HAVING clause
            if token_str.upper() in ['ORDER', 'LIMIT', ';']:
                if condition_parts:
                    self._parse_having_conditions(' '.join(condition_parts), query)
                return idx

            if not token.is_whitespace:
                condition_parts.append(token_str)

            idx += 1

        if condition_parts:
            self._parse_having_conditions(' '.join(condition_parts), query)

        return idx

    def _parse_having_conditions(self, condition_str: str, query: SQLQuery):
        """Parse HAVING conditions"""
        # Parse aggregate conditions
        match = re.match(
            r'(\w+)\s*\(\s*([\w.]+)\s*\)\s*([=<>!]+)\s*(.+)',
            condition_str,
            re.IGNORECASE
        )

        if match:
            agg_func = match.group(1).upper()
            attr_str = match.group(2)
            operator = match.group(3)
            value = match.group(4).strip()

            # Parse attribute
            if '.' in attr_str:
                parts = attr_str.split('.')
                relation = parts[0]
                name = parts[1]
            else:
                relation = ""
                name = attr_str

            aggregate = self.aggregate_map.get(agg_func)
            attribute = Attribute(
                relation=relation,
                name=name,
                aggregate=aggregate
            )

            having_cond = WhereCondition(
                attribute=attribute,
                operator=operator,
                value=value,
                is_join=False
            )
            query.having.append(having_cond)

    def _parse_order_by_clause(self, tokens: List, start_idx: int, query: SQLQuery) -> int:
        """Parse ORDER BY clause"""
        idx = start_idx
        current_attr = []
        direction = "ASC"

        while idx < len(tokens):
            token = tokens[idx]
            token_str = str(token).strip().upper()

            # Check for end of ORDER BY clause
            if token_str in ['LIMIT', 'OFFSET', ';']:
                if current_attr:
                    self._add_order_by_attribute(current_attr, direction, query)
                return idx

            # Check for direction
            if token_str in ['ASC', 'DESC']:
                direction = token_str
            elif token_str == ',':
                if current_attr:
                    self._add_order_by_attribute(current_attr, direction, query)
                    current_attr = []
                    direction = "ASC"
            elif not tokens[idx].is_whitespace:
                current_attr.append(str(tokens[idx]))

            idx += 1

        if current_attr:
            self._add_order_by_attribute(current_attr, direction, query)

        return idx

    def _add_order_by_attribute(self, attr_parts: List[str], direction: str, query: SQLQuery):
        """Add ORDER BY attribute to query"""
        attr_str = ' '.join(attr_parts).replace('ASC', '').replace('DESC', '').strip()

        if '.' in attr_str:
            parts = attr_str.split('.')
            relation = parts[0].strip()
            name = parts[1].strip()
        else:
            relation = ""
            name = attr_str.strip()

        attribute = Attribute(relation=relation, name=name)
        query.order_by.append((attribute, direction))

    def _parse_limit_clause(self, tokens: List, start_idx: int, query: SQLQuery) -> int:
        """Parse LIMIT clause"""
        if start_idx < len(tokens):
            token_str = str(tokens[start_idx]).strip()
            if token_str.isdigit():
                query.limit = int(token_str)
                return start_idx + 1
        return start_idx

    def _parse_offset_clause(self, tokens: List, start_idx: int, query: SQLQuery) -> int:
        """Parse OFFSET clause"""
        if start_idx < len(tokens):
            token_str = str(tokens[start_idx]).strip()
            if token_str.isdigit():
                query.offset = int(token_str)
                return start_idx + 1
        return start_idx

    def _parse_insert_query(self, parsed: TokenList, query: SQLQuery):
        """Parse INSERT query"""
        # Extract INSERT INTO table_name (columns) VALUES (values)
        query_str = str(parsed).strip()

        # Match INSERT pattern
        pattern = r'INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)'
        match = re.match(pattern, query_str, re.IGNORECASE)

        if match:
            query.insert_table = match.group(1)

            # Parse columns
            columns = [col.strip() for col in match.group(2).split(',')]

            # Parse values
            values = [val.strip().strip("'\"") for val in match.group(3).split(',')]

            # Create insert values dictionary
            for col, val in zip(columns, values):
                query.insert_values[col] = val

    def _parse_delete_query(self, parsed: TokenList, query: SQLQuery):
        """Parse DELETE query"""
        query_str = str(parsed).strip()

        # Match DELETE FROM pattern
        pattern = r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
        match = re.match(pattern, query_str, re.IGNORECASE)

        if match:
            query.delete_table = match.group(1)
            query.from_tables = [query.delete_table]

            # Parse WHERE conditions if present
            if match.group(2):
                self._parse_conditions(match.group(2), query)