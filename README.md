# HDFS, Hive Metastore, and Spark Tech Stack Migration
Legacy RDBMS systems are running data pipelines and ETL jobs that create data sets used by various stakeholders ranging from data scientists to business teams. Running unique counts on very large volumes of data frequently is costly with a traditional RDBMS. Storage costs and maintenance costs are very high. Scheduled jobs running egress and ETL are also more costly. The advantages of moving to a No-SQL data warehouse and new tech stack like Spark, Hive, and Trino (previously known as Presto) are evident. Resource management is much easier and can be done at a job level by adjusting the spark.yml file and the job's configuration parameters. Storage costs are much less. Data is typically stored in parquet files in HDFS. Data sets are then registered in Hive via Spark jobs and then consumed by BI tools and platforms like Tableau. Not too mention the new environment runs on the cloud and Kubernetes. 

There are certainly pros and reasons for using an RDBMS but the requirements for this particular project could not be met with an RDBMS. There are 2 existing frameworks that will be transfered to the HDFS (top of the funnel) model. The first is a series of BTEQ scripts stored on a remote server that are scheduled through crontab. Any changes were made directly to the files in production and clearly is not a sustainable data engineering model with no GitHub integration meaning not even a continuous integration system where all the code is stored in a centralized repository with version control.

The other framework involves a slightly more sophisticated pipeline that utilizes Apache's Airflow for scheduling ETL jobs but still against an RDBMS. The jobs however, do now have continuous integration made possible with GitHub. Code changes have to be tested in a dev branch and then merged to the master branch which does not require approval. This at least keeps all the code in a central repository with version control. There is no deployment time as well which is a definite benefit here but at the cost of not having a 'demo' or 'dev' environment. The testing would be conducted locally. Airflow relies on what are called DAG files that contain all the properties and directions for the job but ultimately is a fairly simple python wrapper. 

### Now that we've discussed what the existing landscape looks like, let's talk a bit about what the future holds.
Data will be stored in HDFS in parquet format. These data will be processed by Spark for ETL jobs, ML jobs, and more. For this project, we are going to be focusing on ETL jobs. The Spark job's output is then written back to HDFS and then registered in the Hive Metastore. What is the Hive Metastore? Trino can then be used to query the data in HMS. BI tools like Tableau have data connectors that allow users to connect to various data sources to use in their analaysis. Tableau has a Presto connector which can be used with Trino. The system that runs the Spark jobs is a CI/CD (Continuous Deployment) system with multiple environments that is powered by Kuberenetes. This system uses GitHub, PySpark application, .yml files and a python virtual environment. The metadata for each job is stored in a JSON file, properties for spark jobs and pipelines stored in .rml files, jobs run by starting up PySpark applications (also supports Scala), and CI/CD managed through GitHub integration and Actions. Given the volume of data and the computations used in generating the final data sets being able to store data either in memory or disk as Spark data frames and being able to scale up resources using Spark configuration properties and utilizing join capabilities like broadcasting were really big wins resulting in equivalent and in some cases better performance than the prior frameworks. The telemetry results were better than the other frameworks on top of the infrastrusture and the role of DevOps in the new tech stack.

### Walkthrough

