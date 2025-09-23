"""
SPARQL Executor - Executes SPARQL queries on RDF stores
Supports multiple backends including AllegroGraph, Fuseki, and in-memory RDFLib
"""
from typing import Dict, List, Any, Optional, Union
from enum import Enum
import json
import requests
from SPARQLWrapper import SPARQLWrapper, JSON, POST, GET, BASIC
from rdflib import Graph, Namespace
from rdflib.plugins.sparql import prepareQuery


class StoreType(Enum):
    """Supported RDF store types"""
    ALLEGROGRAPH = "allegrograph"
    FUSEKI = "fuseki"
    BLAZEGRAPH = "blazegraph"
    VIRTUOSO = "virtuoso"
    RDFLIB = "rdflib"  # In-memory store


class SPARQLExecutor:
    """
    Executes SPARQL queries on various RDF stores
    """

    def __init__(
        self,
        store_type: StoreType = StoreType.RDFLIB,
        endpoint: str = None,
        username: str = None,
        password: str = None,
        graph: Graph = None
    ):
        """
        Initialize SPARQL executor

        Args:
            store_type: Type of RDF store
            endpoint: SPARQL endpoint URL
            username: Username for authentication
            password: Password for authentication
            graph: RDFLib Graph for in-memory operations
        """
        self.store_type = store_type
        self.endpoint = endpoint
        self.username = username
        self.password = password

        # Initialize based on store type
        if store_type == StoreType.RDFLIB:
            self.graph = graph or Graph()
        elif endpoint:
            self.sparql = SPARQLWrapper(endpoint)
            if username and password:
                self.sparql.setCredentials(username, password)
                self.sparql.setHTTPAuth(BASIC)
        else:
            raise ValueError(f"Endpoint required for {store_type}")

    def execute_query(self, sparql_query: str) -> Union[Dict, List, bool]:
        """
        Execute a SPARQL query

        Args:
            sparql_query: SPARQL query string

        Returns:
            Query results (format depends on query type)
        """
        # Determine query type
        query_lower = sparql_query.strip().lower()

        if query_lower.startswith('select'):
            return self._execute_select(sparql_query)
        elif query_lower.startswith('insert'):
            return self._execute_update(sparql_query)
        elif query_lower.startswith('delete'):
            return self._execute_update(sparql_query)
        elif query_lower.startswith('ask'):
            return self._execute_ask(sparql_query)
        elif query_lower.startswith('construct'):
            return self._execute_construct(sparql_query)
        elif query_lower.startswith('describe'):
            return self._execute_describe(sparql_query)
        else:
            raise ValueError(f"Unknown query type: {sparql_query[:20]}...")

    def _execute_select(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute SELECT query

        Args:
            query: SPARQL SELECT query

        Returns:
            List of result rows as dictionaries
        """
        if self.store_type == StoreType.RDFLIB:
            return self._execute_rdflib_select(query)
        else:
            return self._execute_remote_select(query)

    def _execute_update(self, query: str) -> bool:
        """
        Execute UPDATE query (INSERT/DELETE)

        Args:
            query: SPARQL UPDATE query

        Returns:
            True if successful
        """
        if self.store_type == StoreType.RDFLIB:
            return self._execute_rdflib_update(query)
        else:
            return self._execute_remote_update(query)

    def _execute_ask(self, query: str) -> bool:
        """
        Execute ASK query

        Args:
            query: SPARQL ASK query

        Returns:
            Boolean result
        """
        if self.store_type == StoreType.RDFLIB:
            result = self.graph.query(query)
            return result.askAnswer
        else:
            self.sparql.setQuery(query)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()
            return results.get('boolean', False)

    def _execute_construct(self, query: str) -> Graph:
        """
        Execute CONSTRUCT query

        Args:
            query: SPARQL CONSTRUCT query

        Returns:
            RDF Graph with constructed triples
        """
        if self.store_type == StoreType.RDFLIB:
            result = self.graph.query(query)
            return result.graph
        else:
            self.sparql.setQuery(query)
            self.sparql.setReturnFormat(JSON)
            results = self.sparql.query().convert()
            # Convert JSON-LD to Graph
            g = Graph()
            g.parse(data=json.dumps(results), format='json-ld')
            return g

    def _execute_describe(self, query: str) -> Graph:
        """
        Execute DESCRIBE query

        Args:
            query: SPARQL DESCRIBE query

        Returns:
            RDF Graph with description
        """
        return self._execute_construct(query)

    def _execute_rdflib_select(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute SELECT query on in-memory RDFLib graph

        Args:
            query: SPARQL SELECT query

        Returns:
            List of result rows
        """
        results = []
        qres = self.graph.query(query)

        # Get variable names
        var_names = [str(var) for var in qres.vars]

        # Convert results to dictionaries
        for row in qres:
            result_dict = {}
            for i, var in enumerate(var_names):
                value = row[i]
                if value is not None:
                    result_dict[var] = str(value)
                else:
                    result_dict[var] = None
            results.append(result_dict)

        return results

    def _execute_rdflib_update(self, query: str) -> bool:
        """
        Execute UPDATE query on in-memory RDFLib graph

        Args:
            query: SPARQL UPDATE query

        Returns:
            True if successful
        """
        try:
            self.graph.update(query)
            return True
        except Exception as e:
            print(f"Update failed: {e}")
            return False

    def _execute_remote_select(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute SELECT query on remote SPARQL endpoint

        Args:
            query: SPARQL SELECT query

        Returns:
            List of result rows
        """
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        self.sparql.setMethod(GET)

        results = self.sparql.query().convert()

        # Parse results
        rows = []
        for result in results["results"]["bindings"]:
            row = {}
            for var in result:
                value = result[var]["value"]
                row[var] = value
            rows.append(row)

        return rows

    def _execute_remote_update(self, query: str) -> bool:
        """
        Execute UPDATE query on remote SPARQL endpoint

        Args:
            query: SPARQL UPDATE query

        Returns:
            True if successful
        """
        try:
            if self.store_type == StoreType.ALLEGROGRAPH:
                return self._execute_allegrograph_update(query)
            else:
                # Generic SPARQL UPDATE
                self.sparql.setQuery(query)
                self.sparql.setMethod(POST)
                self.sparql.query()
                return True
        except Exception as e:
            print(f"Update failed: {e}")
            return False

    def _execute_allegrograph_update(self, query: str) -> bool:
        """
        Execute UPDATE query on AllegroGraph

        Args:
            query: SPARQL UPDATE query

        Returns:
            True if successful
        """
        # AllegroGraph specific update endpoint
        update_endpoint = self.endpoint.replace('/sparql', '/update')

        headers = {
            'Content-Type': 'application/sparql-update'
        }

        auth = None
        if self.username and self.password:
            auth = (self.username, self.password)

        response = requests.post(
            update_endpoint,
            data=query,
            headers=headers,
            auth=auth
        )

        return response.status_code == 200

    def load_data(self, file_path: str, format: str = "turtle"):
        """
        Load RDF data from file

        Args:
            file_path: Path to RDF file
            format: RDF format
        """
        if self.store_type == StoreType.RDFLIB:
            self.graph.parse(file_path, format=format)
        else:
            # For remote stores, would need to use their specific bulk load APIs
            raise NotImplementedError(
                f"Bulk loading not implemented for {self.store_type}"
            )

    def get_statistics(self) -> Dict[str, int]:
        """
        Get statistics about the RDF store

        Returns:
            Dictionary with statistics
        """
        stats = {}

        # Count total triples
        count_query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
        result = self.execute_query(count_query)
        if result:
            stats['total_triples'] = int(result[0].get('count', 0))

        # Count distinct subjects
        subj_query = "SELECT (COUNT(DISTINCT ?s) as ?count) WHERE { ?s ?p ?o }"
        result = self.execute_query(subj_query)
        if result:
            stats['distinct_subjects'] = int(result[0].get('count', 0))

        # Count distinct predicates
        pred_query = "SELECT (COUNT(DISTINCT ?p) as ?count) WHERE { ?s ?p ?o }"
        result = self.execute_query(pred_query)
        if result:
            stats['distinct_predicates'] = int(result[0].get('count', 0))

        # Count distinct types
        type_query = """
        SELECT (COUNT(DISTINCT ?type) as ?count)
        WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type }
        """
        result = self.execute_query(type_query)
        if result:
            stats['distinct_types'] = int(result[0].get('count', 0))

        return stats

    def test_connection(self) -> bool:
        """
        Test connection to the RDF store

        Returns:
            True if connection is successful
        """
        try:
            # Simple ASK query
            test_query = "ASK { ?s ?p ?o }"
            result = self.execute_query(test_query)
            return isinstance(result, bool)
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False