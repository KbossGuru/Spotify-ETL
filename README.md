# Spotify-ETL
An end-to-end data engineering project utilizing Python and Airflow. The project involves extracting data from the Spotify API, transforming it using Python, and loading it into Amazon S3. The entire process is automated and orchestrated using Apache Airflow, running on a daily schedule.

## Objective
To build a data pipeline that automatically fetches Spotify playlist data, processes it, and uploads it to Amazon S3 on a daily basis. This data can then be used for further analysis or visualization, such as creating dashboards in Power BI.

## Architecture
![Project Architecture](https://github.com/KbossGuru/Spotify-ETL/blob/main/Spotify.png)

## Components
1. Spotify API Integration:
    - Purpose: To fetch playlist data from Spotify, including track details, artist information, and album details.
    - Credentials: Spotify API credentials are used to authenticate and access playlist data.

2. Data Extraction and Processing:
    - Extraction Function (extract_and_store_data):
        - Fetches the playlist tracks from Spotify using the playlist ID.
        - Collects details such as track name, artist, album, release date, popularity, genre, album cover URL, and track length.
        - Converts the collected data into a pandas DataFrame.
    - Transformation Function (transform_data):
        - Rounds the track length to two decimal places.
        - Prepares the DataFrame for storage.

3. Amazon S3 Integration:
    - Purpose: To store the processed CSV file containing the playlist data.
    - Credentials: AWS credentials are used to access and interact with S3.
    - Upload Function (load_data_to_s3):
        - Converts the DataFrame to a CSV format and uploads it to an S3 bucket.

4. Apache Airflow Pipeline:
    - Purpose: To automate the data extraction, transformation, and loading (ETL) process on a daily basis.
    - Tasks:
        - Extract Task: Executes the extract_and_store_data function to fetch and prepare the data.
        - Transform Task: Executes the transform_data function to process the data.
        - Load Task: Executes the load_data_to_s3 function to upload the processed data to S3.
        - Task Dependencies: Ensures that the extraction happens before transformation, and transformation happens before loading the data to S3.
      
## Steps in the Pipeline
1. Authentication:
    - Authenticate with the Spotify API using client credentials.
    - Configure AWS credentials for accessing S3.
    
2. Data Extraction:
    - Fetch playlist tracks from Spotify.
    - Extract relevant data fields and compile them into a DataFrame.
    
3. Data Transformation:
    - Process the DataFrame to round values and format data as required.

4. Data Loading:
    - Convert the DataFrame to a CSV file format.
    - Upload the CSV file to the specified S3 bucket.

5. Scheduling:
    - Set up Airflow to execute the ETL pipeline daily, ensuring that the data is refreshed and updated regularly.

6. Error Handling and Logging
    - Error Handling:
        - Implement try-except blocks in your Python functions to catch and log errors.
        - Ensure that errors during API calls or data processing are logged for debugging.
    - Logging:
        -  Use Python’s logging module to capture detailed logs during execution.
        - Configure Airflow’s logging to store and view logs through the Airflow web interface.
