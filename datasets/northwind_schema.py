#!/usr/bin/env python3
"""
Northwind Schema Mapper for SQL2SPARQL
Maps SQL table names to RDF classes and columns to properties
"""

from typing import Dict, Any
from sql2sparql.core.schema_mapper import SchemaMapper

class NorthwindSchemaMapper(SchemaMapper):
    """Schema mapper for Northwind dataset"""
    
    def __init__(self):
        super().__init__()
        self._setup_northwind_mappings()
    
    def _setup_northwind_mappings(self):
        """Setup Northwind-specific table and column mappings"""
        
        # Table to RDF class mappings
        self.table_mappings = {
            'customer': 'http://northwind.example.org/ontology/Customer',
            'product': 'http://northwind.example.org/ontology/Product', 
            'orders': 'http://northwind.example.org/ontology/Order',
            'orderdetail': 'http://northwind.example.org/ontology/OrderDetail',
            'category': 'http://northwind.example.org/ontology/Category',
            'supplier': 'http://northwind.example.org/ontology/Supplier',
            'employee': 'http://northwind.example.org/ontology/Employee',
            'shipper': 'http://northwind.example.org/ontology/Shipper'
        }
        
        # Column to RDF property mappings
        self.column_mappings = {
            # Customer columns
            'customerID': 'http://northwind.example.org/ontology/customerID',
            'companyName': 'http://northwind.example.org/ontology/companyName',
            'contactName': 'http://northwind.example.org/ontology/contactName',
            'contactTitle': 'http://northwind.example.org/ontology/contactTitle',
            'address': 'http://northwind.example.org/ontology/address',
            'city': 'http://northwind.example.org/ontology/city',
            'region': 'http://northwind.example.org/ontology/region',
            'postalCode': 'http://northwind.example.org/ontology/postalCode',
            'country': 'http://northwind.example.org/ontology/country',
            'phone': 'http://northwind.example.org/ontology/phone',
            'fax': 'http://northwind.example.org/ontology/fax',
            
            # Product columns
            'productID': 'http://northwind.example.org/ontology/productID',
            'productName': 'http://northwind.example.org/ontology/productName',
            'quantityPerUnit': 'http://northwind.example.org/ontology/quantityPerUnit',
            'unitPrice': 'http://northwind.example.org/ontology/unitPrice',
            'unitsInStock': 'http://northwind.example.org/ontology/unitsInStock',
            'unitsOnOrder': 'http://northwind.example.org/ontology/unitsOnOrder',
            'reorderLevel': 'http://northwind.example.org/ontology/reorderLevel',
            'discontinued': 'http://northwind.example.org/ontology/discontinued',
            
            # Order columns
            'orderID': 'http://northwind.example.org/ontology/orderID',
            'orderDate': 'http://northwind.example.org/ontology/orderDate',
            'requiredDate': 'http://northwind.example.org/ontology/requiredDate',
            'shippedDate': 'http://northwind.example.org/ontology/shippedDate',
            'freight': 'http://northwind.example.org/ontology/freight',
            'shipName': 'http://northwind.example.org/ontology/shipName',
            'shipAddress': 'http://northwind.example.org/ontology/shipAddress',
            'shipCity': 'http://northwind.example.org/ontology/shipCity',
            'shipRegion': 'http://northwind.example.org/ontology/shipRegion',
            'shipPostalCode': 'http://northwind.example.org/ontology/shipPostalCode',
            'shipCountry': 'http://northwind.example.org/ontology/shipCountry',
            
            # OrderDetail columns
            'unitPrice': 'http://northwind.example.org/ontology/unitPrice',
            'quantity': 'http://northwind.example.org/ontology/quantity',
            'discount': 'http://northwind.example.org/ontology/discount',
            
            # Category columns
            'categoryID': 'http://northwind.example.org/ontology/categoryID',
            'categoryName': 'http://northwind.example.org/ontology/categoryName',
            'description': 'http://northwind.example.org/ontology/description',
            
            # Supplier columns
            'supplierID': 'http://northwind.example.org/ontology/supplierID',
            'homePage': 'http://northwind.example.org/ontology/homePage',
            
            # Employee columns
            'employeeID': 'http://northwind.example.org/ontology/employeeID',
            'lastName': 'http://northwind.example.org/ontology/lastName',
            'firstName': 'http://northwind.example.org/ontology/firstName',
            'title': 'http://northwind.example.org/ontology/title',
            'titleOfCourtesy': 'http://northwind.example.org/ontology/titleOfCourtesy',
            'birthDate': 'http://northwind.example.org/ontology/birthDate',
            'hireDate': 'http://northwind.example.org/ontology/hireDate',
            'homePhone': 'http://northwind.example.org/ontology/homePhone',
            'extension': 'http://northwind.example.org/ontology/extension',
            'photo': 'http://northwind.example.org/ontology/photo',
            'notes': 'http://northwind.example.org/ontology/notes',
            'reportsTo': 'http://northwind.example.org/ontology/reportsTo',
            'photoPath': 'http://northwind.example.org/ontology/photoPath',
            
            # Shipper columns
            'shipperID': 'http://northwind.example.org/ontology/shipperID'
        }
        
        # Join relationship mappings
        self.join_mappings = {
            # Customer -> Orders
            ('customer', 'customerID', 'orders', 'customerID'): 'http://northwind.example.org/ontology/customer',
            
            # Orders -> OrderDetail
            ('orders', 'orderID', 'orderdetail', 'orderID'): 'http://northwind.example.org/ontology/order',
            
            # OrderDetail -> Product
            ('orderdetail', 'productID', 'product', 'productID'): 'http://northwind.example.org/ontology/product',
            
            # Product -> Category
            ('product', 'categoryID', 'category', 'categoryID'): 'http://northwind.example.org/ontology/category',
            
            # Product -> Supplier
            ('product', 'supplierID', 'supplier', 'supplierID'): 'http://northwind.example.org/ontology/supplier',
            
            # Employee -> Orders
            ('employee', 'employeeID', 'orders', 'employeeID'): 'http://northwind.example.org/ontology/employee',
            
            # Shipper -> Orders
            ('shipper', 'shipperID', 'orders', 'shipVia'): 'http://northwind.example.org/ontology/shipVia'
        }
        
        # Primary key mappings
        self.primary_keys = {
            'customer': 'customerID',
            'product': 'productID',
            'orders': 'orderID',
            'orderdetail': ('orderID', 'productID'),  # Composite key
            'category': 'categoryID',
            'supplier': 'supplierID',
            'employee': 'employeeID',
            'shipper': 'shipperID'
        }
    
    def get_table_class(self, table_name: str) -> str:
        """Get RDF class for SQL table"""
        return self.table_mappings.get(table_name.lower(), f"http://northwind.example.org/ontology/{table_name.capitalize()}")
    
    def get_column_property(self, column_name: str, table_name: str | None = None) -> str:
        """Get RDF property for SQL column"""
        return self.column_mappings.get(column_name, f"http://northwind.example.org/ontology/{column_name}")
    
    def get_join_property(self, left_table: str, left_column: str, right_table: str, right_column: str) -> str:
        """Get RDF property for join relationship"""
        key = (left_table.lower(), left_column.lower(), right_table.lower(), right_column.lower())
        return self.join_mappings.get(key, f"http://northwind.example.org/ontology/{left_column}")
    
    def get_primary_key(self, table_name: str) -> str:
        """Get primary key for table"""
        pk = self.primary_keys.get(table_name.lower())
        if pk is None:
            return f"{table_name}ID"
        elif isinstance(pk, tuple):
            return pk[0]  # Return first column of composite key
        return pk