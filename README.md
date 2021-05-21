# HDFS, Hive Metastore, and Spark Tech Stack Migration
Legacy RDBMS systems are running data pipelines and ETL jobs that create data sets used by various stakeholders ranging from data scientists to business teams. Running unique counts on very large volumes of data frequently is costly with a traditional RDBMS. Storage costs and maintenance costs are very high. Scheduled jobs running egress and ETL are also more costly. The advantages of moving to a No-SQL data warehouse and new tech stack like Spark, Hive, and Trino (previously known as Presto) are evident. Resource management is much easier and can be done at a job level by adjusting the spark.yml file and the job's configuration parameters. Storage costs are much less. Data is typically stored in parquet files in HDFS. Data sets are then registered in Hive via Spark jobs and then consumed by BI tools and platforms like Tableau. Not too mention the new environment runs on the cloud and Kubernetes. 

There are certainly pros and reasons for using an RDBMS but the requirements for this particular project could not be met with an RDBMS. There are 2 existing frameworks that will be transfered to the HDFS (top of the funnel) model. The first is a series of BTEQ scripts stored on a remote server that are scheduled through crontab. Any changes were made directly to the files in production and clearly is not a sustainable data engineering model with no GitHub integration meaning not even a continuous integration system where all the code is stored in a centralized repository with version control.

The other framework involves a slightly more sophisticated pipeline that utilizes Apache's Airflow for scheduling ETL jobs but still against an RDBMS. The jobs however, do now have continuous integration made possible with GitHub. Code changes have to be tested in a dev branch and then merged to the master branch which does not require approval. This at least keeps all the code in a central repository with version control. There is no deployment time as well which is a definite benefit here but at the cost of not having a 'demo' or 'dev' environment. The testing would be conducted locally. Airflow relies on what are called DAG files that contain all the properties and directions for the job but ultimately is a fairly simple python wrapper. 

### Now that we've discussed what the existing landscape looks like, let's talk a bit about what the future holds.
Data will be stored in HDFS in parquet format. These data will be processed by Spark for ETL jobs, ML jobs, and more. For this project, we are going to be focusing on ETL jobs. The Spark job's output is then written back to HDFS and then registered in the Hive Metastore. What is the Hive Metastore? Trino can then be used to query the data in HMS. BI tools like Tableau have data connectors that allow users to connect to various data sources to use in their analaysis. Tableau has a Presto connector which can be used with Trino. The system that runs the Spark jobs is a CI/CD (Continuous Deployment) system with multiple environments that is powered by Kuberenetes. This system uses GitHub, PySpark application, .yml files and a python virtual environment. The metadata for each job is stored in a JSON file, properties for spark jobs and pipelines stored in .rml files, jobs run by starting up PySpark applications (also supports Scala), and CI/CD managed through GitHub integration and Actions. Given the volume of data and the computations used in generating the final data sets being able to store data either in memory or disk as Spark data frames and being able to scale up resources using Spark configuration properties and utilizing join capabilities like broadcasting were really big wins resulting in equivalent and in some cases better performance than the prior frameworks. The telemetry results were better than the other frameworks on top of the infrastrusture and the role of DevOps in the new tech stack.

### Walkthrough

All the frameworks did employ various levels of DevOps behaviors such as using Python’s try/except functions and logging and monitoring. Data availability is critical for most ETL jobs as there is some reliance on upstream data sets. In order to reduce the lag of data freshness, a push system would be ideal where the next job automatically starts when the data becomes available. This is somewhat of a hybrid push pull system. A task or job is created as a dependancy and this job is tasked with checking the data availability of the upstream data sources. If the test passes and the data is available, then the Spark job continues on to the next stage or job. 

After we know what journal entry to check for our data, we can create this job in PySpark and deploy it on our data pipelines platform. For example,

First, it is important to describe what the job is, who the owner of it is, and when it runs.

<pre>
######################################################################## 
# Owner: 
# Frequency: 
# Job Description: 
# 
########################################################################
</pre>

Then we import the relevant packages. There is no need for a separate requirements.txt file unless you have custom packages you need to install otherwise there is one main requirements.txt file for the entire platform. Each job essentially spins up a new virtual environment so installing packages is not cumbersome. 

<pre>
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import os
from pyspark import SparkConf, SparkContext
import pandas as pd
import sys
import pytz
import dateutil.relativedelta
</pre>

Next we want to incorporate our logic and test case in a function. We want this to be a function so that the platform does not invoke the code until the function is called and this is also why we use:

<pre>if __name__ == "__main__”:</pre>

Our test case might look something like the below:

<pre>
def main(spark, in_dt):
    df = spark.sql('''
    select count(1) as num_records
    from hms.journal
    where check_dt cob = ‘{0}'
    '''.format(in_dt))
    if df.count() == 0:
        raise RuntimeError("Logging Info *** Source data not available for {0}".format(in_dt))
    else:
        print("Logging Info *** Source data available for {0}".format(in_dt))
        return True
</pre>

Let’s break this down a little bit further. We created a parameter, in_dt, so that when deployed in our pipeline (done via Docker) we can use that as a parameter and pass the latest date through. Also, note here that journal is an HMS or Hive Metastore table. We can review the underlying properties of the table by running something like:

<pre>
spark.sql('''
    describe table extended hms.journal
    '’’).show(300,False)
</pre>

This will also tell you the underlying directory and partition keys in HDFS.

Another point of interest in the main function’s code is the logging statements.

<pre>raise RuntimeError("Logging Info *** Source data not available for {0}".format(in_dt))</pre>

These statements are designed in such a way because Splunk is the provider being used for logs so in order to query the logs easily and efficiently, I use a distinct pattern such as

<pre>Logging Info ***</pre>

Logging is extremely important and can help debug and troubleshoot more effectively as well as provide custom and accessible monitoring. The overall concept here is that we want this job to fail if the data is not ready and provide us with a message indicating this.

How does simply raising a RuntimeError provide this type of notification? That would be because we use a .yml to configure the spark jobs and pipelines and can configure this file so that an email is sent upon failure.

<pre>
    - id: pipeline_id_123
      name: 'weekly (MON): 07:00 AM - [job_name]'
      depends_on: [‘precheck']
      # use the following property to set a schedule for your pipeline. if doing so, you must also define the schedule as shown below
      schedules: ['schedule-weekly-mon-07-00-am']
      notifications: ['notification-10']
</pre>

We can also configure the job to retry N amount of times. This can be useful in cases where let’s say you have a window from 5am to 7am, you expect the source data to be available. You might set the configuration of the job so it starts at 5am and continues to retry upon failure.

<pre>
# spark.pie.restart.on.failure: 'true',
# spark.pie.max.num.restarts.on.failure: '4',
# spark.pie.backoff.duration.on.failure.ms: '300000',
# spark.pie.job.duration.reset.restart.counter.ms: '900000',
</pre>
