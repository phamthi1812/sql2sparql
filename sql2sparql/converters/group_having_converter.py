"""
GROUP BY and HAVING Converters - Converts SQL GROUP BY and HAVING clauses to SPARQL
Based on algorithms from Tables XI (ConvSqlGroupBy) and XII (ConvSqlHaving) in the paper
"""
from typing import List, Dict, Tuple, Optional
from ..core.models import Attribute, Triple, WhereCondition, AggregateFunction


class GroupHavingConverter:
    """
    Converts SQL GROUP BY and HAVING clauses to SPARQL equivalents
    Implements ConvSqlGroupBy() and ConvSqlHaving() algorithms from the paper
    """

    def __init__(self, schema_mapper=None):
        """
        Initialize GROUP BY and HAVING converter

        Args:
            schema_mapper: SchemaMapper instance for resolving predicates
        """
        self.schema_mapper = schema_mapper
        self.group_by_vars: List[str] = []
        self.having_conditions: List[str] = []
        self.additional_patterns: List[Triple] = []
        self.subject_vars: Dict[str, str] = {}

    def convert_group_by(
        self,
        group_by_attributes: List[Attribute],
        existing_patterns: Optional[List[Triple]] = None,
        select_vars: Optional[List[str]] = None,
        select_patterns: Optional[List[Triple]] = None
    ) -> Tuple[List[str], List[Triple]]:
        """
        Convert SQL GROUP BY clause to SPARQL GROUP BY
        Implementation of ConvSqlGroupBy() algorithm from Table XI

        Args:
            group_by_attributes: List of GROUP BY attributes
            existing_patterns: Existing triple patterns

        Returns:
            Tuple of (GROUP BY variables, additional triple patterns)
        """
        group_vars = []
        additional_patterns = []

        # Extract subject variables from existing patterns
        if existing_patterns:
            self._extract_subject_vars(existing_patterns)

        for attr in group_by_attributes:
            # Get or create subject variable
            subject_var = self._get_subject_var(attr.relation)

            if attr.name.lower() == 'subject':
                # Group by subject itself
                group_vars.append(subject_var)

                # Add type pattern if not already present
                if not self._pattern_exists(existing_patterns, subject_var, "rdf:type"):
                    type_uri = self._get_type_uri(attr.relation)
                    type_pattern = Triple(
                        subject=subject_var,
                        predicate="rdf:type",
                        object=type_uri
                    )
                    additional_patterns.append(type_pattern)

            else:
                # Group by attribute value - use SELECT variable if available
                object_var = None
                
                # Try to find matching variable in SELECT vars
                if select_vars:
                    object_var = self._find_select_var_for_attribute(attr, select_vars, select_patterns or [])
                
                # If not found in SELECT, create new variable
                if object_var is None:
                    object_var = f"?{attr.name}_group"
                    
                    # Check if pattern already exists in existing patterns
                    predicate_uri = self._get_predicate_uri(attr.name)
                    if not self._pattern_exists(existing_patterns or [], subject_var, predicate_uri):
                        # Create new pattern for this attribute
                        pattern = Triple(
                            subject=subject_var,
                            predicate=predicate_uri,
                            object=object_var
                        )
                        additional_patterns.append(pattern)
                    else:
                        # Find the existing object variable
                        found_object_var = self._find_object_var(
                            existing_patterns or [], subject_var, predicate_uri
                        )
                        object_var = found_object_var if found_object_var else f"?{attr.name}_group"

                group_vars.append(object_var)

        self.group_by_vars = group_vars
        self.additional_patterns = additional_patterns

        return group_vars, additional_patterns

    def _find_select_var_for_attribute(
        self, 
        attr: Attribute, 
        select_vars: List[str], 
        select_patterns: List[Triple]
    ) -> Optional[str]:
        """
        Find the SELECT variable that corresponds to a GROUP BY attribute
        
        Args:
            attr: GROUP BY attribute
            select_vars: List of SELECT variables
            select_patterns: List of SELECT triple patterns
            
        Returns:
            Matching SELECT variable or None
        """
        # Look through SELECT patterns to find matching attribute
        for i, pattern in enumerate(select_patterns):
            if (pattern.predicate and attr.name in pattern.predicate and 
                i < len(select_vars)):
                return select_vars[i]
        
        # Also try to match by variable name patterns
        for var in select_vars:
            # Remove ? prefix and check if it matches attribute name
            var_name = var.lstrip('?')
            if attr.name.lower() in var_name.lower():
                return var
                
        return None

    def convert_having(
        self,
        having_conditions: List[WhereCondition],
        existing_patterns: Optional[List[Triple]] = None,
        select_vars: Optional[List[str]] = None,
        select_patterns: Optional[List[Triple]] = None
    ) -> List[str]:
        """
        Convert SQL HAVING clause to SPARQL HAVING
        Implementation of ConvSqlHaving() algorithm from Table XII

        Args:
            having_conditions: List of HAVING conditions
            existing_patterns: Existing triple patterns

        Returns:
            List of HAVING condition strings
        """
        having_exprs = []

        # Extract subject variables from existing patterns
        if existing_patterns:
            self._extract_subject_vars(existing_patterns)

        for cond in having_conditions:
            attr = cond.attribute
            subject_var = self._get_subject_var(attr.relation)

            # Build aggregate expression
            if attr.is_aggregate() and attr.aggregate:
                if attr.name.lower() == 'subject':
                    # Aggregate on subject
                    agg_expr = self._build_aggregate_expr(
                        subject_var, attr.aggregate
                    )
                else:
                    # Try to find matching variable in SELECT vars first
                    object_var = None
                    if select_vars:
                        object_var = self._find_select_var_for_attribute(attr, select_vars, select_patterns or [])
                    
                    # If not found in SELECT, look in existing patterns
                    if object_var is None:
                        predicate_uri = self._get_predicate_uri(attr.name)
                        object_var = self._find_object_var(
                            existing_patterns or [], subject_var, predicate_uri
                        )

                    if not object_var:
                        # Create new variable as last resort
                        object_var = f"?{attr.name}_having"

                    if attr.aggregate:
                        # For HAVING, always build a fresh aggregate expression
                        # using the base variable, not the SELECT alias
                        if 'AS' in object_var and '(' in object_var:
                            # Extract base variable from SELECT expression
                            import re
                            match = re.search(r'\?(\w+)', object_var)
                            base_var = match.group(0) if match else object_var
                            agg_expr = self._build_aggregate_expr(
                                base_var, attr.aggregate
                            )
                        else:
                            agg_expr = self._build_aggregate_expr(
                                object_var, attr.aggregate
                            )
                    else:
                        continue

                # Build HAVING condition
                having_expr = f"{agg_expr} {cond.operator} {cond.value}"
                having_exprs.append(having_expr)

        self.having_conditions = having_exprs
        return having_exprs

    def _extract_subject_vars(self, patterns: List[Triple]):
        """Extract subject variables from triple patterns"""
        for pattern in patterns:
            # Simple extraction - in production, would need proper mapping
            if pattern.subject.startswith("?s"):
                self.subject_vars[pattern.subject] = pattern.subject

    def _get_subject_var(self, relation: str) -> str:
        """
        Get or create subject variable for a relation

        Args:
            relation: Relation/table name

        Returns:
            Subject variable name
        """
        # Try to find existing subject variable
        for var in self.subject_vars.values():
            # Simple matching - in production would need proper relation tracking
            return var

        # Create new subject variable
        var_index = len(self.subject_vars)
        new_var = f"?s{var_index}"
        self.subject_vars[relation] = new_var
        return new_var

    def _pattern_exists(
        self,
        patterns: Optional[List[Triple]],
        subject: str,
        predicate: str
    ) -> bool:
        """
        Check if a pattern with given subject and predicate exists

        Args:
            patterns: List of triple patterns
            subject: Subject variable
            predicate: Predicate URI

        Returns:
            True if pattern exists, False otherwise
        """
        if not patterns:
            return False

        for pattern in patterns:
            if pattern.subject == subject and pattern.predicate == predicate:
                return True
        return False

    def _find_object_var(
        self,
        patterns: Optional[List[Triple]],
        subject: str,
        predicate: str
    ) -> Optional[str]:
        """
        Find object variable for a given subject and predicate

        Args:
            patterns: List of triple patterns
            subject: Subject variable
            predicate: Predicate URI

        Returns:
            Object variable if found, None otherwise
        """
        if not patterns:
            return None

        for pattern in patterns:
            if pattern.subject == subject and pattern.predicate == predicate:
                return pattern.object
        return None

    def _build_aggregate_expr(self, variable: str, aggregate: AggregateFunction) -> str:
        """
        Build SPARQL aggregate expression

        Args:
            variable: Variable to aggregate
            aggregate: Aggregate function

        Returns:
            Aggregate expression string
        """
        agg_map = {
            AggregateFunction.COUNT: "COUNT",
            AggregateFunction.SUM: "SUM",
            AggregateFunction.AVG: "AVG",
            AggregateFunction.MIN: "MIN",
            AggregateFunction.MAX: "MAX"
        }

        agg_func = agg_map.get(aggregate, "COUNT")
        return f"{agg_func}({variable})"

    def _get_predicate_uri(self, attribute_name: str) -> str:
        """
        Get RDF predicate URI for an attribute

        Args:
            attribute_name: Attribute name

        Returns:
            Predicate URI string
        """
        if self.schema_mapper:
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
