#!/usr/bin/env python3
"""
Test complex aggregate queries with GROUP BY and HAVING
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.executors.sparql_executor import SPARQLExecutor, StoreType
from northwind_schema import NorthwindSchemaMapper

def test_complex_aggregates():
    """Test complex aggregate queries"""
    
    # Initialize converter with Northwind schema
    schema_mapper = NorthwindSchemaMapper()
    converter = SQL2SPARQLConverter(schema_mapper)
    
    # Initialize SPARQL executor
    executor = SPARQLExecutor(StoreType.FUSEKI, "http://localhost:3030/northwind/sparql")
    
    # Complex aggregate queries
    test_queries = [
        # Multiple aggregates with GROUP BY
        "SELECT country, COUNT(customerID) as customer_count, COUNT(DISTINCT city) as city_count FROM customer GROUP BY country",
        
        # GROUP BY with HAVING
        "SELECT country, COUNT(customerID) as customer_count FROM customer GROUP BY country HAVING COUNT(customerID) >= 1",
        
        # Aggregate with WHERE filter
        "SELECT country, COUNT(customerID) as customer_count FROM customer WHERE country != 'USA' GROUP BY country",
        
        # Multiple columns in GROUP BY
        "SELECT country, city, COUNT(customerID) as customer_count FROM customer GROUP BY country, city",
        
        # Aggregate with ORDER BY
        "SELECT country, COUNT(customerID) as customer_count FROM customer GROUP BY country ORDER BY customer_count DESC"
    ]
    
    print("Testing Complex Aggregate Queries")
    print("=" * 50)
    
    for i, sql_query in enumerate(test_queries, 1):
        print(f"\nTest {i}: {sql_query}")
        print("-" * 50)
        
        try:
            # Convert SQL to SPARQL
            sparql_query = converter.convert(sql_query)
            print(f"Generated SPARQL:\n{sparql_query}")
            
            # Execute SPARQL query
            results = executor.execute_query(sparql_query)
            
            # Handle different result types
            if isinstance(results, list):
                print(f"Results: {len(results)} rows")
                # Display results
                if results:
                    print("Results:")
                    for j, row in enumerate(results):
                        print(f"  {j+1}: {row}")
            else:
                print(f"Results: {type(results)} - {results}")
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
        
        print()

if __name__ == "__main__":
    test_complex_aggregates()