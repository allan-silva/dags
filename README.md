# dags
My labs - Airflow DAGs

## Misc

Render template args without execute task:

`airflow tasks render [dag id] [task id] [desired execution date]`  

`airflow tasks render cnv_covid19 fetch_vaccination_data 2024-08-07T00:00:00+00:00`
