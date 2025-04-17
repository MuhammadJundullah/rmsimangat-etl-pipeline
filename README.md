## How to install Airflow on Docker | Setting Up Airflow with Docker-Compose

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

