## How to install Airflow on Docker | Setting Up Airflow with Docker-Compose

### **1. Install Docker**
- Download and install Docker Desktop: https://www.docker.com/products/docker-desktop

### **2. Clone Airflow Official Docker Example**
- Jalankan perintah berikut:
  ```bash
  git clone https://github.com/apache/airflow.git
  cd airflow
  cd scripts/in_container/config
  cp .env.example .env
  cd ../../..
  ```

### **3. Jalankan Docker Compose**
- Masuk ke folder `docker-compose`:
  ```bash
  cd airflow/docker-compose
  ```
- Jalankan service:
  ```bash
  docker compose up airflow-init
  docker compose up -d
  ```

### **4. Akses Airflow Web UI**
- Buka browser dan akses: `http://localhost:8080`
- Login menggunakan:
  - Username: `airflow`
  - Password: `airflow`

### **5. Stop Docker**
```bash
docker compose down
```

