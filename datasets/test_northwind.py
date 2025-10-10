#!/usr/bin/env python3
"""
Test SQL2SPARQL conversion with Northwind dataset
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from sql2sparql.core.converter import SQL2SPARQLConverter
from sql2sparql.executors.sparql_executor import SPARQLExecutor, StoreType
from northwind_schema import NorthwindSchemaMapper

def test_northwind_queries():
    """Test SQL2SPARQL conversion with Northwind queries"""
    
    # Initialize converter with Northwind schema
    schema_mapper = NorthwindSchemaMapper()
    converter = SQL2SPARQLConverter(schema_mapper)
    
    # Initialize SPARQL executor
    executor = SPARQLExecutor(StoreType.FUSEKI, "http://localhost:3030/northwind/sparql")
    
    # Test queries
    test_queries = [
        # Simple queries
        "SELECT customerID, companyName, city FROM customer WHERE country = 'Germany'",
        "SELECT productName, unitPrice FROM product WHERE unitPrice > 20",
        
        # Join queries
        "SELECT c.companyName, o.orderID, o.orderDate FROM customer c, orders o WHERE c.customerID = o.customerID",
        
        # Aggregate queries
        "SELECT categoryName, COUNT(productID) as product_count FROM product p, category cat WHERE p.categoryID = cat.categoryID GROUP BY categoryName",
        
        # Complex queries
        "SELECT c.companyName, COUNT(DISTINCT o.orderID) as order_count FROM customer c, orders o WHERE c.customerID = o.customerID GROUP BY c.companyName"
    ]
    
    print("Testing SQL2SPARQL Conversion with Northwind Dataset")
    print("=" * 60)
    
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
            if isinstance(results, bool):
                print(f"Query executed successfully: {results}")
            elif isinstance(results, list):
                print(f"Results: {len(results)} rows")
                # Display first few results
                if results:
                    print("Sample results:")
                    for j, row in enumerate(results[:3]):
                        print(f"  {j+1}: {row}")
                    if len(results) > 3:
                        print(f"  ... and {len(results) - 3} more rows")
            else:
                print(f"Results: {type(results)} - {results}")
            
        except Exception as e:
            print(f"Error: {e}")
        
        print()

if __name__ == "__main__":
    test_northwind_queries()