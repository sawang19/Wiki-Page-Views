# Running the Wiki-Page-Views Project

This document outlines two methods for running and testing the Wiki-Page-Views project.

## Option 1: Testing Online

1. **Access the Application Online**
   - You can test the application's features by visiting: [http://52.38.55.145:8000/](http://52.38.55.145:8000/)

## Option 2: Running the Project Locally (Frontend Only)

### Prerequisites

- Ensure your Python version is greater than 3.8.

### Steps

1. **Navigate to the Frontend Directory**
   - Go to the directory `Wiki-Page-Views/backend/frontend`.

2. **Install NPM Packages**
   - In your terminal, run:
     ```bash
     npm install
     ```

3. **Build the Frontend**
   - Run the following command:
     ```bash
     npm run build
     ```

4. **Install Required Python Packages**
   - Depending on your operating system, install the following packages:
     - django = "*"
     - djangorestframework = "*"
     - pandas = "*"
     - sqlalchemy = "*"
     - mysqlclient = "*"
     - django-cors-headers = "*"

5. **Run the Backend Server**
   - Go to `Wiki-Page-Views/backend`.
   - Run the command:
     ```bash
     python3 manage.py runserver
     ```

6. **Access the Local Application**
   - Go to [localhost:8000](http://localhost:8000) in your web browser to test the project.

---

Choose the method that best suits your needs for testing the Wiki-Page-Views project.
