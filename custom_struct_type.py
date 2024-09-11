import json
from typing import List

class StringType:
    def __str__(self):
        return "StringType"

class IntegerType:
    def __str__(self):
        return "IntegerType"

class DateTimeType:
    def __str__(self):
        return "DateTimeType"

class DoubleType:
    def __str__(self):
        return "DoubleType"

class DecimalType:
    def __str__(self):
        return "DecimalType"

class BooleanType:
    def __str__(self):
        return "BooleanType"

class StructField:
    def __init__(self, name, data_type, nullable=True, description="", sample_values=None):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.description = description
        self.sample_values = sample_values if sample_values else []

    def __str__(self):
        return (f"StructField({self.name}, {self.data_type}, {self.nullable}, "
                f"description={self.description}, sample_values={self.sample_values})")

    def to_dict(self):
        return {
            "name": self.name,
            "data_type": str(self.data_type),
            "nullable": self.nullable,
            "description": self.description,
            "sample_values": self.sample_values
        }

class StructType:
    def __init__(self, fields=None):
        self.fields = fields if fields else []

    def add_field(self, name, data_type, description="", sample_values=None):
        self.fields.append(StructField(name, data_type, description=description, sample_values=sample_values))

    def add_struct(self, name, struct_type, description="", sample_values=None):
        self.fields.append(StructField(name, struct_type, True, description=description, sample_values=sample_values))

    def get_schema(self):
        return self

    def __str__(self):
        fields_str = ", ".join(str(field) for field in self.fields)
        return f"StructType([{fields_str}])"

    def to_dict(self):
        return {
            "fields": [field.to_dict() for field in self.fields]
        }

class TableType:
    def __init__(self, name, schema, layer, description=""):
        self.name = name
        self.schema = schema
        self.layer = layer
        self.description = description

    def __str__(self):
        return json.dumps(self.to_dict())

    def to_dict(self):
        return {
            "name": self.name,
            "schema": self.schema.to_dict(),
            "layer": self.layer,
            "description": self.description
        }

class SampleSchema:
    @staticmethod
    def get_customer_schema():
        # Define a nested structure
        address_schema = StructType()
        address_schema.add_field("street", StringType(), description="Street address", sample_values=["123 Main St", "456 Elm St"])
        address_schema.add_field("city", StringType(), description="City name", sample_values=["Springfield", "Shelbyville"])

        # Define the main structure
        custom_schema = StructType()
        custom_schema.add_field("name", StringType(), description="Full name", sample_values=["John Doe", "Jane Smith"])
        custom_schema.add_field("age", IntegerType(), description="Age in years", sample_values=[30, 25])
        custom_schema.add_field("birthdate", DateTimeType(), description="Birthdate", sample_values=["1990-01-01", "1995-05-15"])
        custom_schema.add_field("salary", DoubleType(), description="Annual salary", sample_values=[50000.0, 60000.0])
        custom_schema.add_field("net_worth", DecimalType(), description="Net worth", sample_values=[100000.00, 200000.00])
        custom_schema.add_field("is_employed", BooleanType(), description="Employment status", sample_values=[True, False])
        custom_schema.add_struct("address", address_schema, description="Address details")

        return json.dumps(custom_schema.to_dict()) #TableType("Customer", custom_schema, "source")
    
    @staticmethod
    def get_orders_schema():
        # Define additional example tables
        orders_schema = StructType()
        orders_schema.add_field("order_id", IntegerType(), description="Order ID", sample_values=[1, 2])
        orders_schema.add_field("customer_id", IntegerType(), description="Customer ID", sample_values=[101, 102])
        orders_schema.add_field("order_date", DateTimeType(), description="Order Date", sample_values=["2023-01-01", "2023-01-02"])
        orders_schema.add_field("total_amount", DecimalType(), description="Total Amount", sample_values=[150.75, 200.50])

        return json.dumps(orders_schema.to_dict()) #TableType("Orders", orders_schema, "source")
    
    @staticmethod
    def get_products_schema():
        products_schema = StructType()
        products_schema.add_field("product_id", IntegerType(), description="Product ID", sample_values=[1001, 1002])
        products_schema.add_field("name", StringType(), description="Product Name", sample_values=["Widget", "Gadget"])
        products_schema.add_field("category", StringType(), description="Category", sample_values=["Tools", "Electronics"])
        products_schema.add_field("price", DoubleType(), description="Price", sample_values=[19.99, 29.99])

        return json.dumps(products_schema.to_dict()) # TableType("Products", products_schema, "source")
    
    @staticmethod
    def get_inventory_schema():
        inventory_schema = StructType()
        inventory_schema.add_field("product_id", IntegerType(), description="Product ID", sample_values=[1001, 1002])
        inventory_schema.add_field("warehouse", StringType(), description="Warehouse Location", sample_values=["A1", "B2"])
        inventory_schema.add_field("stock_level", IntegerType(), description="Stock Level", sample_values=[50, 75])

        return json.dumps(inventory_schema.to_dict()) #TableType("Inventory", inventory_schema, "source")
    
   
    @staticmethod
    def get_source_tables() -> List[dict]:
        """Get the list of source layer tables in JSON format"""
        return [dict(zip(['name','layer','description'],['Customers','source','Customer information'])),
                dict(zip(['name','layer','description'],['Orders','source','Order information'])),
                dict(zip(['name','layer','description'],['Products','source','Product information'])),
                dict(zip(['name','layer','description'],['Inventory','source','Inventory information']))]
    
    @staticmethod
    def get_source_tables_with_columns() -> List[dict]:
        """Get the list of source layer table and his columns in JSON format"""
        
        return ['{"name": "Customers", "layer": "source", "description": "Customer information", "columns": [{"name": "name", "data_type": "StringType", "description": "Full name", "sample_values": ["John Doe", "Jane Smith"]}, {"name": "age", "data_type": "IntegerType", "description": "Age in years", "sample_values": [30, 25]}, {"name": "birthdate", "data_type": "DateTimeType", "description": "Birthdate", "sample_values": ["1990-01-01", "1995-05-15"]}, {"name": "salary", "data_type": "DoubleType", "description": "Annual salary", "sample_values": [50000.0, 60000.0]}, {"name": "net_worth", "data_type": "DecimalType", "description": "Net worth", "sample_values": [100000.0, 200000.0]}, {"name": "is_employed", "data_type": "BooleanType", "description": "Employment status", "sample_values": [true, false]}, {"name": "address", "data_type": "StructType", "description": "Address details"}]}',
                '{"name": "Orders", "layer": "source", "description": "Order information", "columns": [{"name": "order_id", "data_type": "IntegerType", "description": "Order ID", "sample_values": [1, 2]}, {"name": "customer_id", "data_type": "IntegerType", "description": "Customer ID", "sample_values": [101, 102]}, {"name": "order_date", "data_type": "DateTimeType", "description": "Order Date", "sample_values": ["2023-01-01", "2023-01-02"]}, {"name": "total_amount", "data_type": "DecimalType", "description": "Total Amount", "sample_values": [150.75, 200.5]}]}',
                '{"name": "Products", "layer": "source", "description": "Product information", "columns": [{"name": "product_id", "data_type": "IntegerType", "description": "Product ID", "sample_values": [1001, 1002]}, {"name": "name", "data_type": "StringType", "description": "Product Name", "sample_values": ["Widget", "Gadget"]}, {"name": "category", "data_type": "StringType", "description": "Category", "sample_values": ["Tools", "Electronics"]}, {"name": "price", "data_type": "DoubleType", "description": "Price", "sample_values": [19.99, 29.99]}]}',
                '{"name": "Inventory", "layer": "source", "description": "Inventory information", "columns": [{"name": "product_id", "data_type": "IntegerType", "description": "Product ID", "sample_values": [1001, 1002]}, {"name": "warehouse", "data_type": "StringType", "description": "Warehouse Location", "sample_values": ["A1", "B2"]}, {"name": "stock_level", "data_type": "IntegerType", "description": "Stock Level", "sample_values": [50, 75]}]}']
    


