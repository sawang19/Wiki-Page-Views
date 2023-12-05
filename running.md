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


Choose the method that best suits your needs for testing the Wiki-Page-Views project.

# Wikipedia Pageviews Data Processing Steps

## 1. Downloading Data
- **Script**: `PJ_download_v0.6.py`
- **Location**: `Wiki-Page-Views\extractfile`
- **Customization**:
  - **Starting Date**: Modify `num = 20230701 + i` to your desired starting date.
  - **Data Range**: Change the range in `for i in range(0,1):` to the number of days you want to process. For 5 days, use `range(0,5)`.
- **Output**: The script filters, splits, and zips the data, storing it in the script's directory. Data for each day is stored in a separate folder.

## 2. ETL Operations

### ETL.py
- **Required Packages**: zipfile, shutil, nltk
- **Estimated Running Time**: 1.5 hours for one-month data
- **Instructions**:
  - Save the `ETL.py` script in the same folder as the data files for a specific month (e.g., `pageviews-2023MM01-user.zip` to `pageviews-2023MM30(28/31)-user.zip`).
  - Run `spark-submit ETL.py` or `sudo python3 ETL.py`.
  - **Output**: `pageviews-2023MM-bymonth.zip`

### extractkeyword.py
- **Estimated Running Time**: 1 hour for one-month data
- **Instructions**:
  - Store `extractkeyword.py` with `pageviews-2023MM-bymonth.zip` in the same directory.
  - Execute `spark-submit extractkeyword.py 2023MM` or `sudo python3 extractkeyword.py 2023MM`, replacing ‘MM’ with the month number.
  - **Output**: `keywords-2023MM-bymonth.zip`

### top10.py
- **Estimated Running Time**: 40 minutes for one-month data
- **Instructions**:
  - Place `top10.py` with `pageviews-2023MM-bymonth.zip` in the same folder.
  - Run `spark-submit top10.py 2023MM` or `sudo python3 top10.py 2023MM`, substituting ‘MM’ with the specific month.
  - **Output**: `top_titles_byday_2023MM.zip`

### Notes:
- Ensure all required packages are installed before running the scripts.
- Adjust date and time ranges as per your data requirements.
- Check for sufficient storage space for handling large data files.