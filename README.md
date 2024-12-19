# Cquirrel Implementation for TPC-H Query 5 and Query 10  

## Overview  

This project implements the **Cquirrel algorithm** for incremental query maintenance in real-time streaming environments using **Apache Flink**. The implementation focuses on **TPC-H Query 5** and **Query 10**, efficiently maintaining results over foreign-key acyclic schemas with stateful processing. The solution avoids redundant computations by propagating delta updates for dynamic data changes.  

## Features  

- **Incremental Query Maintenance**:  
   - Real-time update propagation across multi-way joins.  
   - Efficient filtering, joining, and aggregation with low overhead.  

- **Supported Queries**:  
   - **Query 5**: Computes supplier revenue within a specific region and date range.  
   - **Query 10**: Calculates customer-specific revenue metrics with conditions on orders and line items.  

- **Scalable Deployment**:  
   - Deployed via **Dockerized Apache Flink** for distributed and scalable processing.  

## Experiment Environment  

The project has been tested in the following environment:  

- **Java**: JDK 11  
- **Apache Flink**: 1.18.0  
- **Maven**: 3.10  
- **Environment**: WSL2 (Windows Subsystem for Linux)  
- **Deployment**: Docker  

## Directory Structure  

```plaintext
Cquirrel-TPC-H/
│
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── charles/
│   │   │           ├── main/                    # Query 5 and Query 10 logic
│   │   │           │   ├── Q5Main.java          # Entry point for Query 5
│   │   │           │   ├── Q10Main.java         # Entry point for Query 10
│   │   │           │   ├── Q5AggregateProcessFunction.java
│   │   │           │   ├── Q5CustomerProcessFunction.java
│   │   │           │   ├── Q5IntermediateProcessFunction.java
│   │   │           │   ├── Q5LineitemProcessFunction.java
│   │   │           │   ├── Q5NationProcessFunction.java
│   │   │           │   ├── Q5OrdersProcessFunction.java
│   │   │           │   ├── Q5RegionProcessFunction.java
│   │   │           │   ├── Q5SupplierProcessFunction.java
│   │   │           │   ├── Q10AggregateProcessFunction.java
│   │   │           │   ├── Q10CustomerProcessFunction.java
│   │   │           │   ├── Q10LineitemProcessFunction.java
│   │   │           │   ├── Q10NationProcessFunction.java
│   │   │           │   └── Q10OrderProcessFunction.java
│   │   │           └── RelationType/
│   │   │               └── Payload.java         # Data structure for tuple processing
│   │   └── resources/                           # Configuration files
│
├── Dockerfile                                   # Docker setup for Flink environment
├── docker-compose.yml                           # Docker Compose configuration for Flink cluster
├── pom.xml                                      # Maven configuration file
└── README.md                                    # Project documentation
```
# How to Run
## Build the Project
Use maven to build and package jar:
```
mvn clean package
```
## Deploy Flink with Docker
Start a Flink cluster using Docker Compose:
```
docker-compose up -d
```
Recommend to use Docker Desktop to install docker in wsl2.
## Run Query 5 or Query 10
```
Submit the compiled jar, Then set the entry as 
com.charles.main.Q10Main
OR
com.charles.main.Q5Main

para: --input [input_file]  --output [output_file]
```
## Monitor Execution
Access the Flink dashboard at http://localhost:8081 (My docker-compose setting, is 8082 port) to monitor the job execution and progress.
