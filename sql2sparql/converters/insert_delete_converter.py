"""
INSERT and DELETE Converters - Converts SQL INSERT/DELETE to SPARQL
Based on algorithms from Tables XIII-XV in the paper
"""
from typing import List, Dict, Any, Tuple, Optional
import uuid
from ..core.models import Triple, WhereCondition


class InsertDeleteConverter:
    """
    Converts SQL INSERT and DELETE queries to SPARQL UPDATE operations
    Implements getTP(), generateInsert(), and ConvDeleteSql() algorithms from the paper
    """

    def __init__(self, schema_mapper=None, base_uri: str = "http://example.org/"):
        """
        Initialize INSERT/DELETE converter

        Args:
            schema_mapper: SchemaMapper instance for resolving predicates
            base_uri: Base URI for generating new resources
        """
        self.schema_mapper = schema_mapper
        self.base_uri = base_uri

    def convert_insert(
        self,
        table_name: str,
        values: Dict[str, Any]
    ) -> List[Triple]:
        """
        Convert SQL INSERT to SPARQL INSERT DATA
        Implementation of getTP() and generateInsert() algorithms from Tables XIII and XIV

        Args:
            table_name: Table to insert into
            values: Dictionary of column names to values

        Returns:
            List of triples to insert
        """
        triples = []

        # Generate a new subject URI for this entity
        subject_uri = self._generate_subject_uri(table_name)

        # Add type triple
        type_uri = self._get_type_uri(table_name)
        type_triple = Triple(
            subject=f"<{subject_uri}>",
            predicate="rdf:type",
            object=type_uri
        )
        triples.append(type_triple)

        # Convert each attribute-value pair to a triple
        for attribute, value in values.items():
            if attribute.lower() == 'subject':
                # Skip subject column as we generate our own URIs
                continue

            # Get predicate URI for this attribute
            predicate_uri = self._get_predicate_uri(attribute)

            # Format value based on type
            formatted_value = self._format_value(value)

            # Create triple
            triple = Triple(
                subject=f"<{subject_uri}>",
                predicate=predicate_uri,
                object=formatted_value
            )
            triples.append(triple)

        return triples

    def convert_delete(
        self,
        table_name: str,
        where_conditions: List[WhereCondition],
        join_conditions: Optional[List] = None
    ) -> Tuple[List[Triple], List[Triple], List[str]]:
        """
        Convert SQL DELETE to SPARQL DELETE WHERE
        Implementation of ConvDeleteSql() algorithm from Table XV

        Args:
            table_name: Table to delete from
            where_conditions: WHERE conditions for deletion
            join_conditions: JOIN conditions (if any)

        Returns:
            Tuple of (delete patterns, where patterns, filter conditions)
        """
        delete_patterns = []
        where_patterns = []
        filter_conditions = []

        # Create subject variable
        subject_var = "?s0"

        # Add type pattern to identify entities to delete
        type_uri = self._get_type_uri(table_name)
        type_pattern = Triple(
            subject=subject_var,
            predicate="rdf:type",
            object=type_uri
        )
        where_patterns.append(type_pattern)

        # If no conditions, delete all triples for entities of this type
        if not where_conditions:
            # Delete pattern: ?s0 ?p ?o (all properties)
            delete_all_pattern = Triple(
                subject=subject_var,
                predicate="?p",
                object="?o"
            )
            delete_patterns.append(delete_all_pattern)

            # Also delete the type triple
            delete_patterns.append(type_pattern)

        else:
            # Process WHERE conditions
            for cond in where_conditions:
                attr = cond.attribute

                if attr.name.lower() == 'subject':
                    # Filter on subject URI itself
                    filter_expr = self._build_filter_expression(
                        subject_var, cond.operator, cond.value
                    )
                    filter_conditions.append(filter_expr)

                    # Delete all properties of matching subjects
                    delete_pattern = Triple(
                        subject=subject_var,
                        predicate="?p",
                        object="?o"
                    )
                    delete_patterns.append(delete_pattern)

                else:
                    # Create pattern for the attribute
                    object_var = f"?{attr.name}_del"
                    predicate_uri = self._get_predicate_uri(attr.name)

                    # WHERE pattern to match entities
                    where_pattern = Triple(
                        subject=subject_var,
                        predicate=predicate_uri,
                        object=object_var
                    )
                    where_patterns.append(where_pattern)

                    # DELETE pattern for matched entities
                    delete_pattern = Triple(
                        subject=subject_var,
                        predicate=predicate_uri,
                        object=object_var
                    )
                    delete_patterns.append(delete_pattern)

                    # Add FILTER condition
                    filter_expr = self._build_filter_expression(
                        object_var, cond.operator, cond.value
                    )
                    filter_conditions.append(filter_expr)

            # Also delete type triple for matched entities
            delete_patterns.append(type_pattern)

        return delete_patterns, where_patterns, filter_conditions

    def _generate_subject_uri(self, table_name: str) -> str:
        """
        Generate a new subject URI for an entity

        Args:
            table_name: Table/type name

        Returns:
            Generated URI string
        """
        # Use UUID for unique identifier
        entity_id = uuid.uuid4().hex[:8]
        return f"{self.base_uri}{table_name}/{entity_id}"

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
            return f"<{self.base_uri}ontology/{attribute_name}>"
        else:
            return f"<{self.base_uri}ontology/{attribute_name}>"

    def _get_type_uri(self, table_name: str) -> str:
        """
        Get RDF type URI for a table

        Args:
            table_name: Table name

        Returns:
            Type URI string
        """
        if self.schema_mapper:
            return f"<{self.base_uri}types/{table_name.title()}>"
        else:
            return f"<{self.base_uri}types/{table_name.title()}>"

    def _format_value(self, value: Any) -> str:
        """
        Format a value for RDF

        Args:
            value: Value to format

        Returns:
            Formatted value string
        """
        if value is None:
            return '""'

        # Check if it's a URI
        if isinstance(value, str) and (value.startswith('http://') or value.startswith('https://')):
            return f"<{value}>"

        # Check if it's a number
        if isinstance(value, (int, float)):
            return str(value)

        # Check if string value is a number
        if isinstance(value, str):
            try:
                float(value)
                return value  # Return as-is if it's numeric
            except ValueError:
                pass  # Not a number, continue

        # Check if it's a boolean
        if isinstance(value, bool):
            return str(value).lower()

        # Default to string literal
        # Escape quotes and special characters
        value_str = str(value).replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
        return f'"{value_str}"'

    def _build_filter_expression(self, variable: str, operator: str, value: Any) -> str:
        """
        Build SPARQL FILTER expression

        Args:
            variable: Variable name
            operator: Comparison operator
            value: Value to compare against

        Returns:
            FILTER expression string
        """
        # Format value
        formatted_value = self._format_value(value)

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
            return f"{variable} {sparql_op} {formatted_value}"
