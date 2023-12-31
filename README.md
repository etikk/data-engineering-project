### Data Engineering Project: Analyzing Scientific Publications

Fall 2023/2024
LTAT.02.007
University of Tartu

================================
Team 17:
Erkki Tikk
Mattias Väli
Kristjan Laht
Anton Malkovski
================================

### Project Overview

This project is part of the Data Engineering course, focused on designing and implementing a data pipeline to analyze scientific publications data. The key objectives include:

- Building a multi-faceted analytical view on scientific publications data.
- Creating a data warehouse/data mart for Business Intelligence (BI) queries.
- Utilizing a graph database for co-authorship prediction and community analysis.
- Implementing various data transformations, cleansing, and augmentation processes.

### Technologies and Systems Used

- **Programming Language**: Python
- **Data Storage**:
  - PostgreSQL/MySQL for the data warehouse.
  - Neo4J for the graph database.
- **Data Pipeline Management**: Apache Airflow.
- **Web Interface**: pgAdmin for PostgreSQL management.
- **Containerization**: Docker and Docker Compose.
- **Libraries and Tools**:
  - Python libraries like Pandas, Requests, SQLAlchemy, and Py2neo.
  - Data transformation tools such as `scholar.py`, Crossref API, etc.

---

## Docker Setup Instructions

### Prerequisites

- Docker and Docker Compose installed on your machine.
- Clone the project repository.
- Obtain a `.env` file with the necessary environment variables from the project maintainer.

### Steps to Setup and Run the Project

1. **Clone the Repository**

   - Clone the project repository to your local machine.
   - Navigate to the project directory.

2. **The `.env` File**

   - We have added the `.env` file to git as this is a educational project with no sensitive data.
   - In a production environment, uncomment the .env line in .gitignore and change all the passwords.

3. **Build and Run Docker Containers**

   - In the terminal, run:
     ```
     docker-compose up -d
     ```
   - This command builds and starts the Docker containers defined in the `docker-compose.yml` file.

4. **Accessing pgAdmin**

   - Open a web browser and navigate to `http://localhost:5050`.
   - Log in using the credentials provided in the `.env` file.

5. **Setting Up the Database in pgAdmin**

   - In pgAdmin, right-click on 'Servers' -> 'Create' -> 'Server'.
   - Under the 'General' tab, give your server a name.
   - In the 'Connection' tab, set:
     - Host name/address: `db` (as defined in the Docker Compose file)
     - Port: `5432`
     - Maintenance database: (as specified in `.env` under `POSTGRES_DB`)
     - Username: (as specified in `.env` under `POSTGRES_USER`)
     - Password: (as specified in `.env` under `POSTGRES_PASSWORD`)
   - Click 'Save' to establish the connection.

6. **Accessing Neo4J Browser**

   - Open a web browser and navigate to `http://localhost:7474`.
   - Log in using the credentials specified in the `.env` file (`NEO4J_USERNAME` and `NEO4J_PASSWORD`).
   - You can utilize the Neo4J import directory (`/var/lib/neo4j/import`) to import your data if needed. We have a DAG for import in AirFlow.
   - You can use Cypher queries within the Neo4J Browser or connect via a Neo4j client in your application.

7. **Setting Up AirFlow**

- Run the initialization script (`init_airflow.sh` or `init_airflow.bat` depending on the platform) to set up their local Airflow environment.
- Run 'docker-compose up --build -d' to build the new containers and start Airflow.

### PDF report

The PDF report is also found in the root of this repo. 

---
