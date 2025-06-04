# NOAA_Weather

This project investigates long-term climate trend by analyzing 60 years of global daily weather data sourced from NOAA (National Oceanic and Atmospheric Administration). The data has been cleaned, enriched, and stored in BigQuery, with downstream transformation and analysis performed via dbt, and visualized in Looker studio.

For simplicity, this project includes Spark within the Airflow container. During orchestration, Airflow is responsible for starting, connecting to, and stopping the Spark instance automatically. In a production environment, for improved scalability and reliability, Spark should ideally run as a standalone service. Alternatively, GCP offers Dataproc as a service that can spun up Spark clusters as needed.

![Untitled Diagram](https://github.com/user-attachments/assets/4e995b63-f26b-48d9-b3ac-f332aff4aba4)

![image](https://github.com/user-attachments/assets/e38b3baf-1d9e-4e37-a83e-902ff70e602c)
