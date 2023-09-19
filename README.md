# E-Commerce_Brazil- Data Analysis and Visualization

## Project Description

This project focuses on the transformation, analysis, and visualization of E-commerce data from Brazil found on Kaggle. 

Kaggle link:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

It consists of 9 datasets such as :
- Customer
- Geolocation
- Order Items
- Order Payments
- Orders
- Order Reviews
- Products
- Product Category Name
- Sellers

## Data Transformation

The raw data underwent several transformation steps, including:

- Data cleaning to handle missing values.
- Handling all data types accurately and handling all exceptions.
- Removing extra or superficial columns to achieve clean data.
- Aggregation in order to achieve Document Data model as per business requirements

Used Azure Data Factory to fetch data from Azure BLOB, make sufficient transformations and load it into Azure Cosmos DB for Document Data model.
Similarly, utilized python script to load data in Azure Graph using Apache Gremlin API to make neccessary Graph model with vertices and edges. 

## Data Analysis

Our analysis revealed several key findings, such as:

- The 4 top-selling product categories in the Brazilian E-commerce market.
- Seasonal trends in E-commerce sales by the top 4 products.
- Location wise preferences over top 4 product categories.

NoSQL API for Azure Cosmos DB which uses Cosmos QL for Document Data model and GraphQL in Apache Gremlin API for Graph Data Model.

## Data Visualization

Performed Visualization on Power BI with interactive dashboard.

Power BI link:

https://app.powerbi.com/view?r=eyJrIjoiOGM4MWI1MjktZGMwMi00ZjJmLTg1ZjctODVkNTU5MzE5MDY1IiwidCI6ImE4ZWVjMjgxLWFhYTMtNGRhZS1hYzliLTlhMzk4YjkyMTVlNyIsImMiOjN9&embedImagePlaceholder=true

