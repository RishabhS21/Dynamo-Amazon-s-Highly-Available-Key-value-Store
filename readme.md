# **Dynamo: Amazon’s Highly Available Key-value Store**
This repository provides a half-baked implementation of Dynamo which is a highly available key-value store designed by Amazon. This is the [video](https://drive.google.com/file/d/1xXaojWpkTJX2s4Af6lSjnuRCSzr79apB/view?usp=drive_link) explaining this implememntation in detail.

## **Setup Instructions**

Follow these steps to set up the project and run the code on your local machine.

---

## **Prerequisites**

Ensure the following are installed:
- Python 3.7 or later
- PostgreSQL
- `pip` (Python package manager)

---

Also, you need to ensure that you have a server.log file inside logs/ directory.

## **Step 1: Install PostgreSQL**

### **For Ubuntu/Linux**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

### **For macOS**
```bash
brew install postgresql
```

### **For Windows**
1. Download PostgreSQL from the [official website](https://www.postgresql.org/download/).
2. Run the installer and follow the setup wizard.

### **Start PostgreSQL Service**
```bash
sudo service postgresql start
```

### **Set Up PostgreSQL User and Database**
1. Open the PostgreSQL shell:
   ```bash
   sudo -u postgres psql
   ```
2. Run the following commands:
   ```sql
   CREATE USER myuser WITH PASSWORD 'mypassword';
   CREATE DATABASE mydatabase;
   GRANT ALL PRIVILEGES ON DATABASE mydatabase TO myuser;
   ```
3. Exit the PostgreSQL shell:
   ```bash
   \q
   ```

---

## **Step 2: Clone the Project**

```bash
git clone https://github.com/your-repository/serverinfo-sorting.git
cd serverinfo-sorting
```

---

## **Step 3: Install Required Python Modules**

Install all required libraries using `pip`:

```bash
pip install sortedcontainers psycopg2-binary loguru
```

---

## **Step 4: Update Configuration**

Update the PostgreSQL connection settings in the script:

```python
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "mydatabase"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"
```

---

## **Step 5: Run the Script**

Execute the script to verify functionality:

```bash
./run.sh
```
