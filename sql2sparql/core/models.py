"""
Core data models for SQL2SPARQL conversion
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from enum import Enum


class QueryType(Enum):
    """SQL query types supported"""
    SELECT = "SELECT"
    INSERT = "INSERT"
    DELETE = "DELETE"
    UPDATE = "UPDATE"


class AggregateFunction(Enum):
    """Supported aggregate functions"""
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"


class CombinationType(Enum):
    """SQL combination operators"""
    UNION = "UNION"
    INTERSECT = "INTERSECT"
    EXCEPT = "EXCEPT"


@dataclass
class Triple:
    """RDF Triple representation"""
    subject: str
    predicate: str
    object: str

    def to_sparql_pattern(self) -> str:
        """Convert to SPARQL triple pattern"""
        return f"{self.subject} {self.predicate} {self.object}"


@dataclass
class Attribute:
    """SQL attribute representation"""
    relation: str
    name: str
    alias: Optional[str] = None
    aggregate: Optional[AggregateFunction] = None

    def is_aggregate(self) -> bool:
        """Check if attribute has aggregate function"""
        return self.aggregate is not None


@dataclass
class JoinCondition:
    """SQL join condition"""
    left_operand: Attribute
    right_operand: Attribute
    operator: str = "="


@dataclass
class WhereCondition:
    """SQL WHERE condition"""
    attribute: Attribute
    operator: str
    value: Any
    is_join: bool = False


@dataclass
class SQLQuery:
    """Parsed SQL query representation"""
    type: QueryType
    select_attributes: List[Attribute] = field(default_factory=list)
    from_tables: List[str] = field(default_factory=list)
    where_conditions: List[WhereCondition] = field(default_factory=list)
    join_conditions: List[JoinCondition] = field(default_factory=list)
    group_by: List[Attribute] = field(default_factory=list)
    having: List[WhereCondition] = field(default_factory=list)
    order_by: List[Tuple[Attribute, str]] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None

    # For INSERT queries
    insert_table: Optional[str] = None
    insert_values: Dict[str, Any] = field(default_factory=dict)

    # For DELETE queries
    delete_table: Optional[str] = None

    # For combined queries
    combination_type: Optional[CombinationType] = None
    left_query: Optional['SQLQuery'] = None
    right_query: Optional['SQLQuery'] = None


@dataclass
class SPARQLQuery:
    """SPARQL query representation"""
    select_vars: List[str] = field(default_factory=list)
    where_patterns: List[Triple] = field(default_factory=list)
    filter_conditions: List[str] = field(default_factory=list)
    group_by_vars: List[str] = field(default_factory=list)
    having_conditions: List[str] = field(default_factory=list)
    order_by_vars: List[Tuple[str, str]] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None

    # For INSERT DATA
    insert_triples: List[Triple] = field(default_factory=list)

    # For DELETE WHERE
    delete_patterns: List[Triple] = field(default_factory=list)

    def to_string(self) -> str:
        """Convert to SPARQL query string"""
        if self.insert_triples:
            return self._build_insert_query()
        elif self.delete_patterns:
            return self._build_delete_query()
        else:
            return self._build_select_query()

    def _build_select_query(self) -> str:
        """Build SELECT query string"""
        query_parts = []

        # SELECT clause
        select_clause = "SELECT " + " ".join(self.select_vars)
        query_parts.append(select_clause)

        # WHERE clause
        where_clause = "WHERE {\n"
        for pattern in self.where_patterns:
            where_clause += f"  {pattern.to_sparql_pattern()} .\n"

        # FILTER conditions
        for filter_cond in self.filter_conditions:
            where_clause += f"  FILTER({filter_cond})\n"

        where_clause += "}"
        query_parts.append(where_clause)

        # GROUP BY
        if self.group_by_vars:
            query_parts.append("GROUP BY " + " ".join(self.group_by_vars))

        # HAVING
        if self.having_conditions:
            having = "HAVING(" + " && ".join(self.having_conditions) + ")"
            query_parts.append(having)

        # ORDER BY
        if self.order_by_vars:
            order_parts = []
            for var, direction in self.order_by_vars:
                order_parts.append(f"{direction}({var})")
            query_parts.append("ORDER BY " + " ".join(order_parts))

        # LIMIT/OFFSET
        if self.limit:
            query_parts.append(f"LIMIT {self.limit}")
        if self.offset:
            query_parts.append(f"OFFSET {self.offset}")

        return "\n".join(query_parts)

    def _build_insert_query(self) -> str:
        """Build INSERT DATA query string"""
        query = "INSERT DATA {\n"
        for triple in self.insert_triples:
            query += f"  {triple.to_sparql_pattern()} .\n"
        query += "}"
        return query

    def _build_delete_query(self) -> str:
        """Build DELETE WHERE query string"""
        query = "DELETE {\n"
        for pattern in self.delete_patterns:
            query += f"  {pattern.to_sparql_pattern()} .\n"
        query += "}\n"

        query += "WHERE {\n"
        for pattern in self.where_patterns:
            query += f"  {pattern.to_sparql_pattern()} .\n"

        for filter_cond in self.filter_conditions:
            query += f"  FILTER({filter_cond})\n"

        query += "}"
        return query


@dataclass
class RelationalSchema:
    """Extracted relational schema from RDF data"""
    tables: Dict[str, List[str]] = field(default_factory=dict)

    def add_table(self, table_name: str, attributes: List[str]):
        """Add a table with its attributes"""
        self.tables[table_name] = ["subject"] + attributes

    def get_table_attributes(self, table_name: str) -> List[str]:
        """Get attributes for a table"""
        return self.tables.get(table_name, [])
