"""
Main SQL2SPARQL Converter - Orchestrates all conversion components
Based on the convertSqlQuery() algorithm from Table III in the paper
"""
from typing import Optional, Union
from ..parsers.sql_parser import SQLParser
from ..converters.select_converter import SelectConverter
from ..converters.where_converter import WhereConverter
from ..converters.group_having_converter import GroupHavingConverter
from ..converters.insert_delete_converter import InsertDeleteConverter
from .models import SQLQuery, SPARQLQuery, QueryType, CombinationType, Triple
from .schema_mapper import SchemaMapper


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

    def convert(self, sql_query: str) -> str:
        """
        Convert SQL query string to SPARQL query string

        Args:
            sql_query: SQL query string

        Returns:
            SPARQL query string
        """
        # Parse SQL query
        parsed_query = self.sql_parser.parse(sql_query)

        # Convert to SPARQL query object
        sparql_query = self._convert_query(parsed_query)

        # Return as string
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
                sparql_query.where_patterns
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
                sparql_query.where_patterns
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
        insert_triples = self.insert_delete_converter.convert_insert(
            sql_query.insert_table,
            sql_query.insert_values
        )
        sparql_query.insert_triples = insert_triples

    def _convert_delete_query(self, sql_query: SQLQuery, sparql_query: SPARQLQuery):
        """
        Convert DELETE query to SPARQL DELETE WHERE

        Args:
            sql_query: Parsed SQL DELETE query
            sparql_query: SPARQL query object to populate
        """
        # Convert DELETE using algorithm from Table XV
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