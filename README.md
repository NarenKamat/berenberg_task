# berenberg_task
Interview Task 
---------

1. The source code is organised under as -
berenberg_task
|
-- execution_report.py
-- config.txt
-- performance_metrics.py
-- utils
   |
    -- __init__.py
    -- config.py

2. All the configuration for sources are stored under config.txt, this will need to be amended as per the environment
3. Generated Reports are stored  under
   berenberg_task
   |
   -- output_files
      |
       -- execution_report.csv
       -- performance_report.txt

4. Deployment
     Clone the repository using Git

5. Execution
     5a. To generate the execution report run
         python execution_report.py
     5b. To generate the performance metrics report
         python performance_metrics.py
      

Issues:

There referential data issues , cannot find matching listing_id's within the marketdata.parquet 
57 Unique ISIN's dont have a matching bbo data.
