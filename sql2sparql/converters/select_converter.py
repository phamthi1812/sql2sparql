"""
SELECT Clause Converter - Converts SQL SELECT clauses to SPARQL
Based on algorithm from Table X (exConvSqlSelect) in the paper
"""
from typing import List, Dict, Tuple, Optional
from ..core.models import Attribute, Triple, AggregateFunction


class SelectConverter:
    """
    Converts SQL SELECT clauses to SPARQL SELECT clauses with triple patterns
    Implements algorithm from Table X of the paper
    """

    def __init__(self, schema_mapper=None):
        """
        Initialize SELECT converter

        Args:
            schema_mapper: SchemaMapper instance for resolving predicates
        """
        self.schema_mapper = schema_mapper
        self.triple_patterns: List[Triple] = []
        self.variable_counter = 0

    def convert(self, attributes: List[Attribute]) -> Tuple[List[str], List[Triple]]:
        """
        Convert SQL SELECT attributes to SPARQL SELECT variables and triple patterns
        Implementation of exConvSqlSelect() algorithm from Table X

        Args:
            attributes: List of SQL attributes from SELECT clause

        Returns:
            Tuple of (SPARQL variables, triple patterns)
        """
        sparql_vars = []
        triple_patterns = []
        subject_vars: Dict[str, str] = {}  # Track subject variables per relation

        for idx, attr in enumerate(attributes):
            # Get or create subject variable for this relation
            if attr.relation not in subject_vars:
                subject_vars[attr.relation] = f"?s{len(subject_vars)}"

            subject_var = subject_vars[attr.relation]

            if attr.name.lower() == 'subject':
                # Handle subject attribute
                if attr.is_aggregate() and attr.aggregate:
                    # Aggregate on subject
                    var_name = self._apply_aggregate(subject_var, attr.aggregate, attr.alias)
                    sparql_vars.append(var_name)
                else:
                    sparql_vars.append(subject_var)

                # Create type triple pattern
                triple = Triple(
                    subject=subject_var,
                    predicate="rdf:type",
                    object=self._get_type_uri(attr.relation)
                )
                triple_patterns.append(triple)

            else:
                # Handle regular attribute
                object_var = f"?o{idx}"

                if attr.is_aggregate() and attr.aggregate:
                    # Apply aggregate function
                    var_name = self._apply_aggregate(object_var, attr.aggregate, attr.alias)
                    sparql_vars.append(var_name)
                else:
                    sparql_vars.append(object_var)

                # Create triple pattern for this attribute
                predicate_uri = self._get_predicate_uri(attr.name)
                triple = Triple(
                    subject=subject_var,
                    predicate=predicate_uri,
                    object=object_var
                )
                triple_patterns.append(triple)

        self.triple_patterns = triple_patterns
        return sparql_vars, triple_patterns

    def _apply_aggregate(self, variable: str, aggregate: AggregateFunction, alias: Optional[str] = None) -> str:
        """
        Apply aggregate function to a variable

        Args:
            variable: SPARQL variable
            aggregate: Aggregate function enum
            alias: Optional alias for the aggregate result

        Returns:
            Aggregate function call string
        """
        agg_map = {
            AggregateFunction.COUNT: "COUNT",
            AggregateFunction.SUM: "SUM",
            AggregateFunction.AVG: "AVG",
            AggregateFunction.MIN: "MIN",
            AggregateFunction.MAX: "MAX"
        }

        agg_func = agg_map.get(aggregate, "COUNT")

        # Use provided alias or default to variable_agg
        result_alias = f"?{alias}" if alias else f"{variable}_agg"

        return f"({agg_func}({variable}) AS {result_alias})"

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
            # In production, this would look up the actual predicate URI
            return f"<http://example.org/ontology/{attribute_name}>"
        else:
            # Default namespace
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
            # Use schema mapper if available
            return f"<http://example.org/types/{relation_name.title()}>"
        else:
            # Default type namespace
            return f"<http://example.org/types/{relation_name.title()}>"

    def get_triple_patterns(self) -> List[Triple]:
        """
        Get the generated triple patterns

        Returns:
            List of Triple objects
        """
        return self.triple_patterns
