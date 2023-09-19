from gremlin_python.driver import client, serializer, protocol
from gremlin_python.driver.protocol import GremlinServerError
import sys
import traceback
import asyncio
import pandas as pd
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
from typing import List
from azure.storage.blob import BlobClient
import os

# Connect to Azure Blob storage and download the incoming file from Azure Blob 
blob = BlobClient(account_url="https://azureblob7275.blob.core.windows.net",
                  container_name="brazilproject7275",
                  blob_name="CombineCSV5.csv",
                  credential="t9cuuwj4rFSXXEd0p9SQEmWZEomlSU2S2xPSsJ8YJIab2+6lVlfJBetgc9OXSAgftLyy1bYPMAhN+AStN8ZECg==")

original_file_name = "CombineCSV5.csv"
new_file_name = "gremlin_project.csv"

with open(original_file_name, "wb") as f:
    data = blob.download_blob()
    data.readinto(f)

# Check if file exists, if it does then remove it and save it with the new name
if os.path.exists(new_file_name):
    os.remove(new_file_name)
os.rename(original_file_name, new_file_name)

full_csv = pd.read_csv(new_file_name, error_bad_lines=False)

# Tables to create vertices
product = pd.read_csv(new_file_name, usecols=['product_id', 'product_category_name_english', 'product_photos_qty', 
                                                      'product_weight_g', 'product_length_cm', 'product_height_cm', 
                                                      'product_width_cm'])
product = product.drop_duplicates()

order = pd.read_csv(new_file_name, usecols=['order_id', 'customer_unique_id', 'order_estimated_delivery_date', 'order_delivered_customer_date', 
                                                      'order_delivered_carrier_date', 'order_approved_at', 'order_purchase_timestamp', 
                                                      'order_status'], nrows = 10)
order = order.drop_duplicates()

order_review = pd.read_csv(new_file_name, usecols=['review_id', 'review_score', 'review_creation_date'])
order_review = order_review.drop_duplicates()

customer = pd.read_csv(new_file_name, usecols=['customer_unique_id', 'customer_city', 'customer_state', 
                                                      'customer_zip_code_prefix'])

customer['customer_city'] = customer['customer_city'].str.replace("'", " ")
customer = customer.drop_duplicates(subset=['customer_unique_id'], keep='first')

payment = pd.read_csv(new_file_name, usecols=['payment_sequential', 'payment_type', 'payment_installments', 
                                                      'payment_value'])
payment = payment.drop_duplicates()
payment['payment_SK'] = payment['payment_sequential'].astype(str) +'_' + payment['payment_type'].astype(str) +'_' + payment['payment_installments'].astype(str) + '_' + payment['payment_value'].astype(str)


# Mapping tables to create edges
order_product = pd.read_csv(new_file_name, usecols=['product_id', 'order_id'])
order_product = order_product.drop_duplicates()

order_payment = pd.read_csv(new_file_name, usecols=['order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value'])
order_payment['payment_SK'] = order_payment['payment_sequential'].astype(str) + '_' + order_payment['payment_type'].astype(str) + '_' + order_payment['payment_installments'].astype(str) + '_' + order_payment['payment_value'].astype(str)
order_payment = order_payment.drop_duplicates()

order_customer = pd.read_csv(new_file_name, usecols=['customer_unique_id', 'order_id'])
order_customer = order_customer.drop_duplicates()

order_order_review = pd.read_csv(new_file_name, usecols=['review_id', 'order_id'])
order_order_review = order_order_review.drop_duplicates()


if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_gremlin_cleanup_graph = "g.V().drop()"

_gremlin_count_vertices = "g.V().count()"

def print_status_attributes(result):
    print("\tResponse status_attributes:\n\t{0}".format(result.status_attributes))

def cleanup_graph(client):
    print("\n> {0}".format(
        _gremlin_cleanup_graph))
    callback = client.submitAsync(_gremlin_cleanup_graph)
    if callback.result() is not None:
        callback.result().all().result() 
    print("\n")
    print_status_attributes(callback.result())
    print("\n")


def insert_vertices(client, all_vertices):
    """
        The function "insert_vertices" takes two arguments: "client" and "all_vertices".
        Args:
        - client: client object used to submit queries to a graph database. 
        - all_vertices: list of queries that insert vertices into the graph database.
        
        The function iterates over each query in the "all_vertices" list and submits it to the database using the client object.
        
        Returns:
        - The result of each query is checked for errors and if successful, the inserted vertex is printed.
        - The function also calls a helper function "print_status_attributes" to print the status and attributes of the inserted vertex.
    """
    for query in all_vertices:
        print("\n> {0}\n".format(query))
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tInserted this vertex:\n\t{0}".format(
                callback.result().all().result()))
        else:
            print("Something went wrong with this query: {0}".format(query))
        print("\n")
        print_status_attributes(callback.result())
        print("\n")

    print("\n")

def create_gremlin_vertices_query(df, vertex_label: str, pk_column: str) -> List[str]:
    """
    Generate Gremlin code to insert vertices for each row in a pandas DataFrame
    
    Args:
    - df: pandas DataFrame containing the data to insert
    - vertex_label: str, the label of the vertex to create
    - pk_column: str, the name of the column containing the primary key
    
    Returns:
    - List of strings, each string contains the Gremlin code to insert a single vertex
    """
    gremlin_code = []
    for i, row in df.iterrows():
        pk_value = str(row[pk_column])
        properties = []
        for col, value in row.iteritems():
            if col != pk_column:
                properties.append(f".property('{col}', '{value}')")
        properties = "".join(properties)
        vertex_code = f"g.addV('{vertex_label}').property('id', '{pk_value}').property('_id', '{pk_value}'){properties}"
        gremlin_code.append(vertex_code)
    return gremlin_code
    
def insert_edges(client, all_edges):
    """
        The function "insert_edges" takes two arguments: "client" and "all_edges".
        Args:
        - client: client object used to submit queries to a graph database. 
        - all_edges: list of queries that insert edges into the graph database.
        
        The function iterates over each query in the "all_edges" list and submits it to the database using the client object.
        
        Returns:
        - The result of each query is checked for errors and if successful, the inserted vertex is printed.
        - The function also calls a helper function "print_status_attributes" to print the status and attributes of the inserted vertex.
    """
    for query in all_edges:
        print("\n> {0}\n".format(query))
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tInserted this edge:\n\t{0}\n".format(
                callback.result().all().result()))
        else:
            print("Something went wrong with this query:\n\t{0}".format(query))
        print_status_attributes(callback.result())
        print("\n")

    print("\n")

def create_gremlin_edge_query(df, col1, col2):
    """
    Generate Gremlin code to insert edges for each row in a pandas DataFrame
    
    Args:
    - df: pandas DataFrame containing the edges to insert
    - col1: Primary Key 1 of Table which is the source edge
    - col2: Primary Key 2 of Table which is the destination edge
    
    Returns:
    - List of strings, each string contains the Gremlin code to insert a single edge
    """
    queries = []
    for index, row in df.iterrows():
        source = row[col1]
        dest = row[col2]
        query = f"g.V('{source}').addE('has').to(g.V('{dest}'))"
        queries.append(query)
    return queries

def count_vertices(client):
    print("\n> {0}".format(
        _gremlin_count_vertices))
    callback = client.submitAsync(_gremlin_count_vertices)
    if callback.result() is not None:
        print("\tCount of vertices: {0}".format(callback.result().all().result()))
    else:
        print("Something went wrong with this query: {0}".format(
            _gremlin_count_vertices))

    print("\n")
    print_status_attributes(callback.result())
    print("\n")

try:
    client = client.Client('wss://azure-gremlin-hp.gremlin.cosmos.azure.com:443/', 'g',
                           username="/dbs/gremlin_test/colls/Person2",
                           password="vor028T8qhpJK2Py2ox83j0KQ1CkWPVAMiTirOiU1XQQLeOQ9ss2tNcBeDLOBuunuI9VWAJpAAdmACDbg29Qyg==",
                           message_serializer=serializer.GraphSONSerializersV2d0()
                           )

    print("Azure Cosmos DB + Gremlin on Python!")
    
    print("Let's drop everything first!")
    cleanup_graph(client)

    print("Let's insert vertices now!")    
    insert_vertices(client, create_gremlin_vertices_query(order, 'order', 'order_id'))
    insert_vertices(client, create_gremlin_vertices_query(product, 'product', 'product_id'))
    insert_vertices(client, create_gremlin_vertices_query(order_review, 'order_review', 'review_id'))
    insert_vertices(client, create_gremlin_vertices_query(customer, 'customer', 'customer_unique_id'))
    insert_vertices(client, create_gremlin_vertices_query(payment, 'payment', 'payment_SK'))

    print("Let's insert edges now!")
    insert_edges(client, create_gremlin_edge_query(order_product, 'order_id', 'product_id')) # Order and Product  
    insert_edges(client, create_gremlin_edge_query(order_payment, 'order_id', 'payment_SK')) # Order and Payment
    insert_edges(client, create_gremlin_edge_query(order_customer, 'order_id', 'customer_unique_id')) # Order and Customer
    insert_edges(client, create_gremlin_edge_query(order_order_review, 'order_id', 'review_id')) # Order and Order Review

except GremlinServerError as e:
    print('Code: {0}, Attributes: {1}'.format(e.status_code, e.status_attributes))
    
    cosmos_status_code = e.status_attributes["x-ms-status-code"]
    if cosmos_status_code == 409:
        print('Conflict error!')
    elif cosmos_status_code == 412:
        print('Precondition error!')
    elif cosmos_status_code == 429:
        print('Throttling error!');
    elif cosmos_status_code == 1009:
        print('Request timeout error!')
    else:
        print("Default error handling")

    traceback.print_exc(file=sys.stdout) 
    sys.exit(1)

print("\nWell done!")


