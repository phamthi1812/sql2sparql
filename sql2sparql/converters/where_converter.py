"""
WHERE Clause Converter - Converts SQL WHERE clauses to SPARQL
Based on algorithm from Table VIII (ConvSqlWhere) in the paper
"""
from typing import List, Dict, Tuple, Optional
from ..core.models import (
    WhereCondition, JoinCondition, Triple, Attribute
)


class WhereConverter:
    """
    Converts SQL WHERE clauses to SPARQL WHERE patterns and FILTER conditions
    Implements ConvSqlWhere() algorithm from Table VIII and addJCtoWhere() from Table IX
    """

    def __init__(self, schema_mapper=None):
        """
        Initialize WHERE converter

        Args:
            schema_mapper: SchemaMapper instance for resolving predicates
        """
        self.schema_mapper = schema_mapper
        self.subject_vars: Dict[str, str] = {}
        self.triple_patterns: List[Triple] = []
        self.filter_conditions: List[str] = []

    def convert(
        self,
        join_conditions: List[JoinCondition],
        where_conditions: List[WhereCondition],
        existing_patterns: List[Triple] = None
    ) -> Tuple[List[Triple], List[str]]:
        """
        Convert SQL WHERE clause to SPARQL WHERE patterns
        Implementation of ConvSqlWhere() algorithm from Table VIII

        Args:
            join_conditions: List of join conditions
            where_conditions: List of boolean conditions
            existing_patterns: Triple patterns from SELECT clause

        Returns:
            Tuple of (additional triple patterns, FILTER conditions)
        """
        additional_patterns = []
        filter_conditions = []

        # Initialize subject variables from existing patterns
        if existing_patterns:
            self._extract_subject_vars(existing_patterns)

        # Process join conditions (Algorithm Table IX - addJCtoWhere)
        if join_conditions:
            join_patterns = self._process_join_conditions(join_conditions)
            additional_patterns.extend(join_patterns)

        # Process boolean conditions
        if where_conditions:
            bool_patterns, filters = self._process_boolean_conditions(where_conditions)
            additional_patterns.extend(bool_patterns)
            filter_conditions.extend(filters)

        # If no conditions, return existing patterns
        if not join_conditions and not where_conditions:
            return existing_patterns or [], []

        self.triple_patterns = additional_patterns
        self.filter_conditions = filter_conditions

        return additional_patterns, filter_conditions

    def _extract_subject_vars(self, patterns: List[Triple]):
        """Extract subject variables from existing triple patterns"""
        for pattern in patterns:
            # Extract relation from pattern's subject variable
            # Assuming subject vars are in format ?s0, ?s1, etc.
            self.subject_vars[pattern.subject] = pattern.subject

    def _process_join_conditions(self, join_conditions: List[JoinCondition]) -> List[Triple]:
        """
        Process join conditions and create triple patterns
        Implementation of addJCtoWhere() from Table IX

        Args:
            join_conditions: List of SQL join conditions

        Returns:
            List of triple patterns for joins
        """
        patterns = []

        for join_cond in join_conditions:
            left_op = join_cond.left_operand
            right_op = join_cond.right_operand

            # Get or create subject variables
            left_subj = self._get_subject_var(left_op.relation)
            right_subj = self._get_subject_var(right_op.relation)

            # Handle different join types based on paper's algorithm
            if left_op.name.lower() == 'subject':
                # Left operand is a subject reference
                if right_op.name.lower() == 'subject':
                    # Both are subjects - create equality constraint
                    # This would be handled in FILTER
                    filter_cond = f"{left_subj} = {right_subj}"
                    self.filter_conditions.append(filter_cond)
                else:
                    # Left subject = right attribute
                    # Create pattern: right_subject right_predicate left_subject
                    predicate_uri = self._get_predicate_uri(right_op.name)
                    pattern = Triple(
                        subject=right_subj,
                        predicate=predicate_uri,
                        object=left_subj
                    )
                    patterns.append(pattern)

            elif right_op.name.lower() == 'subject':
                # Right operand is a subject reference
                # Create pattern: left_subject left_predicate right_subject
                predicate_uri = self._get_predicate_uri(left_op.name)
                pattern = Triple(
                    subject=left_subj,
                    predicate=predicate_uri,
                    object=right_subj
                )
                patterns.append(pattern)

            else:
                # Both are attributes - join on common values
                # Create patterns with shared object variable
                shared_var = f"?join_{len(patterns)}"

                # Pattern for left attribute
                left_pred_uri = self._get_predicate_uri(left_op.name)
                left_pattern = Triple(
                    subject=left_subj,
                    predicate=left_pred_uri,
                    object=shared_var
                )
                patterns.append(left_pattern)

                # Pattern for right attribute
                right_pred_uri = self._get_predicate_uri(right_op.name)
                right_pattern = Triple(
                    subject=right_subj,
                    predicate=right_pred_uri,
                    object=shared_var
                )
                patterns.append(right_pattern)

        return patterns

    def _process_boolean_conditions(
        self,
        where_conditions: List[WhereCondition]
    ) -> Tuple[List[Triple], List[str]]:
        """
        Process boolean WHERE conditions

        Args:
            where_conditions: List of boolean conditions

        Returns:
            Tuple of (triple patterns, FILTER conditions)
        """
        patterns = []
        filters = []

        for cond in where_conditions:
            attr = cond.attribute
            subject_var = self._get_subject_var(attr.relation)

            if attr.name.lower() == 'subject':
                # Filter on subject itself
                filter_str = self._build_filter_expression(
                    subject_var, cond.operator, cond.value
                )
                filters.append(filter_str)

                # Add type pattern if needed
                if attr.relation:
                    type_uri = self._get_type_uri(attr.relation)
                    type_pattern = Triple(
                        subject=subject_var,
                        predicate="rdf:type",
                        object=type_uri
                    )
                    patterns.append(type_pattern)

            else:
                # Create pattern for the attribute
                object_var = f"?{attr.name}_value"
                predicate_uri = self._get_predicate_uri(attr.name)

                pattern = Triple(
                    subject=subject_var,
                    predicate=predicate_uri,
                    object=object_var
                )
                patterns.append(pattern)

                # Add FILTER condition
                filter_str = self._build_filter_expression(
                    object_var, cond.operator, cond.value
                )
                filters.append(filter_str)

        return patterns, filters

    def _get_subject_var(self, relation: str) -> str:
        """
        Get or create subject variable for a relation

        Args:
            relation: Relation/table name

        Returns:
            Subject variable name
        """
        if relation not in self.subject_vars:
            var_index = len(self.subject_vars)
            self.subject_vars[relation] = f"?s{var_index}"
        return self.subject_vars[relation]

    def _get_predicate_uri(self, attribute_name: str) -> str:
        """
        Get RDF predicate URI for an attribute

        Args:
            attribute_name: Attribute name

        Returns:
            Predicate URI string
        """
        if self.schema_mapper:
            # Use schema mapper if available
            return f"<http://example.org/ontology/{attribute_name}>"
        else:
            return f"<http://example.org/ontology/{attribute_name}>"

    def _get_type_uri(self, relation_name: str) -> str:
        """
        Get RDF type URI for a relation

        Args:
            relation_name: Relation/table name

        Returns:
            Type URI string
        """
        if self.schema_mapper:
            return f"<http://example.org/types/{relation_name.title()}>"
        else:
            return f"<http://example.org/types/{relation_name.title()}>"

    def _build_filter_expression(self, variable: str, operator: str, value: any) -> str:
        """
        Build SPARQL FILTER expression

        Args:
            variable: Variable name
            operator: Comparison operator
            value: Value to compare against

        Returns:
            FILTER expression string
        """
        # Handle different value types
        if isinstance(value, str):
            # Check if it's a number
            try:
                float(value)
                value_str = value
            except ValueError:
                # String literal - add quotes
                value_str = f'"{value}"'
        else:
            value_str = str(value)

        # Map SQL operators to SPARQL
        operator_map = {
            '=': '=',
            '!=': '!=',
            '<>': '!=',
            '<': '<',
            '>': '>',
            '<=': '<=',
            '>=': '>=',
            'LIKE': 'regex'
        }

        sparql_op = operator_map.get(operator.upper(), operator)

        if sparql_op == 'regex':
            # Convert SQL LIKE to SPARQL regex
            pattern = value.replace('%', '.*').replace('_', '.')
            return f'regex({variable}, "{pattern}", "i")'
        else:
            return f"{variable} {sparql_op} {value_str}"