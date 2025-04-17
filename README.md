## How to install Airflow on Docker | Setting Up Airflow with Docker-Compose

This project is part of my personal portfolio to demonstrate how to build a data ETL pipeline using Apache Airflow. The main goal is to extract data from a MySQL database (used by a web-based restaurant management system called RM Simangat), transform it as needed, and load it into a PostgreSQL data warehouse.

This setup showcases the type of data engineering tasks often done in the industry—where data from transactional systems needs to be regularly ingested and stored in a centralized warehouse for further analysis using BI tools like Tableau, Power BI, or others.

The following instructions explain how to run the project locally using Docker Compose and access the Airflow web interface to manage and monitor your DAGs.

### **1. Install Docker**
- Download and install Docker Desktop from the official website:
  https://www.docker.com/products/docker-desktop
- After installation, make sure Docker is running.

### **2. Clone the Ready-to-Use Repository**
- Clone the repository that already includes the Airflow Docker setup:
  ```bash
  git clone git@github.com:MuhammadJundullah/rmsimangat-etl-pipeline.git
  cd rmsimangat-etl-pipeline
  ```

### **3. Run Docker Compose**
- Initialize Airflow and start the services:
  ```bash
  docker compose up airflow-init
  docker compose up -d
  ```

### **4. Access Airflow Web UI**
- Open your browser and go to: `http://localhost:8080`
- Login credentials:
  - Username: `airflow`
  - Password: `airflow`

### **5. Stop Docker**
```bash
docker compose down
```

