"""
Schema Mapper - Extracts relational schema from RDF data
Based on algorithms from Tables I and II of the paper
"""
from typing import Dict, List, Set, Tuple
from rdflib import Graph, URIRef, Literal, Namespace, RDF
from .models import RelationalSchema, Triple


class SchemaMapper:
    """
    Extracts a relational schema from RDF data to provide SQL users
    with a familiar interface for querying RDF data.
    """

    def __init__(self, rdf_graph: Graph = None):
        """
        Initialize the schema mapper with an RDF graph

        Args:
            rdf_graph: RDFLib Graph object containing RDF data
        """
        self.graph = rdf_graph or Graph()
        self.schema = RelationalSchema()
        self.namespace_map: Dict[str, str] = {}
        self._extracted = False

    def load_rdf_file(self, file_path: str, format: str = "turtle"):
        """
        Load RDF data from file

        Args:
            file_path: Path to RDF file
            format: RDF format (turtle, n3, xml, etc.)
        """
        self.graph.parse(file_path, format=format)
        self._extracted = False

    def load_rdf_string(self, data: str, format: str = "turtle"):
        """
        Load RDF data from string

        Args:
            data: RDF data as string
            format: RDF format
        """
        self.graph.parse(data=data, format=format)
        self._extracted = False

    def extract_schema(self) -> RelationalSchema:
        """
        Extract relational schema from RDF data
        Implementation of Algorithm Part 1 and Part 2 from the paper

        Returns:
            RelationalSchema object containing tables and attributes
        """
        # Step 1: Extract unique predicates per type (Algorithm Part 1)
        type_predicates = self._extract_type_predicates()

        # Step 2: Create tables from types with predicates as attributes (Algorithm Part 2)
        for rdf_type, predicates in type_predicates.items():
            table_name = self._get_table_name(rdf_type)
            self.schema.add_table(table_name, list(predicates))

        self._extracted = True
        return self.schema

    def _extract_type_predicates(self) -> Dict[str, Set[str]]:
        """
        Extract unique predicates for each RDF type
        Implementation of Algorithm Part 1 from Table I

        Returns:
            Dictionary mapping RDF types to their predicates
        """
        type_predicates: Dict[str, Set[str]] = {}

        # Iterate through all triples
        for subj, pred, obj in self.graph:
            # Skip type declarations themselves
            if pred == RDF.type:
                # Initialize set for this type if not exists
                type_str = str(obj)
                if type_str not in type_predicates:
                    type_predicates[type_str] = set()
            else:
                # Find the type(s) of this subject
                subject_types = self._get_subject_types(subj)

                # Add this predicate to each type's predicate set
                for subject_type in subject_types:
                    type_str = str(subject_type)
                    if type_str not in type_predicates:
                        type_predicates[type_str] = set()

                    # Add the predicate to this type's set
                    pred_name = self._get_attribute_name(pred)
                    type_predicates[type_str].add(pred_name)

        return type_predicates

    def _get_subject_types(self, subject: URIRef) -> List[URIRef]:
        """
        Get all RDF types for a given subject

        Args:
            subject: RDF subject URI

        Returns:
            List of RDF type URIs
        """
        types = []
        for s, p, o in self.graph.triples((subject, RDF.type, None)):
            types.append(o)
        return types if types else [URIRef("http://www.w3.org/2002/07/owl#Thing")]

    def _get_table_name(self, rdf_type: str) -> str:
        """
        Convert RDF type URI to table name

        Args:
            rdf_type: RDF type URI

        Returns:
            Table name suitable for SQL
        """
        # Extract local name from URI
        if "#" in rdf_type:
            return rdf_type.split("#")[-1].lower()
        elif "/" in rdf_type:
            return rdf_type.split("/")[-1].lower()
        else:
            return rdf_type.lower()

    def _get_attribute_name(self, predicate: URIRef) -> str:
        """
        Convert RDF predicate URI to attribute name

        Args:
            predicate: RDF predicate URI

        Returns:
            Attribute name suitable for SQL
        """
        pred_str = str(predicate)
        # Extract local name from URI
        if "#" in pred_str:
            return pred_str.split("#")[-1].lower()
        elif "/" in pred_str:
            return pred_str.split("/")[-1].lower()
        else:
            return pred_str.lower()

    def get_triple_patterns(self, table: str, attribute: str) -> Tuple[str, str]:
        """
        Get the RDF predicate URI for a table.attribute reference

        Args:
            table: Table name
            attribute: Attribute name

        Returns:
            Tuple of (predicate_uri, object_variable)
        """
        # This is a simplified mapping - in production, maintain a proper mapping
        # from the extraction phase
        if attribute == "subject":
            return f"rdf:type", f"<http://example.org/{table}>"
        else:
            return f"<http://example.org/{attribute}>", f"?{attribute}_value"

    def get_schema_info(self) -> Dict[str, List[str]]:
        """
        Get schema information as a dictionary

        Returns:
            Dictionary mapping table names to their attributes
        """
        if not self._extracted:
            self.extract_schema()
        return self.schema.tables

    def print_schema(self):
        """Print the extracted schema in a readable format"""
        if not self._extracted:
            self.extract_schema()

        print("\n=== Extracted Relational Schema ===\n")
        for table_name, attributes in self.schema.tables.items():
            print(f"Table: {table_name}")
            print(f"  Attributes: {', '.join(attributes)}")
            print()

    def validate_sql_reference(self, table: str, attribute: str) -> bool:
        """
        Validate if a table.attribute reference exists in the schema

        Args:
            table: Table name
            attribute: Attribute name

        Returns:
            True if valid reference, False otherwise
        """
        if not self._extracted:
            self.extract_schema()

        return (
            table in self.schema.tables
            and attribute in self.schema.tables[table]
        )