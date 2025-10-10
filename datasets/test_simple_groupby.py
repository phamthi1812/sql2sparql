#!/usr/bin/env python3
"""
Test simple GROUP BY queries that should work with current dataset
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.executors.sparql_executor import SPARQLExecutor, StoreType
from northwind_schema import NorthwindSchemaMapper

def test_simple_groupby():
    """Test simple GROUP BY queries"""
    
    # Initialize converter with Northwind schema
    schema_mapper = NorthwindSchemaMapper()
    converter = SQL2SPARQLConverter(schema_mapper)
    
    # Initialize SPARQL executor
    executor = SPARQLExecutor(StoreType.FUSEKI, "http://localhost:3030/northwind/sparql")
    
    # Simple GROUP BY queries that should work
    test_queries = [
        # Count customers by country
        "SELECT country, COUNT(customerID) as customer_count FROM customer GROUP BY country",
        
        # Count products by supplier (using actual relationship)
        "SELECT s.companyName, COUNT(p.productID) as product_count FROM product p, supplier s WHERE p.supplier = s.subject GROUP BY s.companyName",
        
        # Count discontinued vs active products
        "SELECT discontinued, COUNT(productID) as product_count FROM product GROUP BY discontinued"
    ]
    
    print("Testing Simple GROUP BY Queries")
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
        
        print()

if __name__ == "__main__":
    test_simple_groupby()