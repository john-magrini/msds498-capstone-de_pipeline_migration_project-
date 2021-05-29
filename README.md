# HDFS, Hive Metastore, and Spark Tech Stack Migration
Legacy RDBMS systems are running data pipelines and ETL jobs that create data sets used by various stakeholders ranging from data scientists to business teams. Running unique counts on very large volumes of data frequently is costly with a traditional RDBMS. Storage costs and maintenance costs are very high. Scheduled jobs running egress and ETL are also more costly. The advantages of moving to a No-SQL data warehouse and new tech stack like Spark, Hive, and Trino (previously known as Presto) are evident. Resource management is much easier and can be done at a job level by adjusting the spark.yml file and the job's configuration parameters. Storage costs are much less. Data is typically stored in parquet files in HDFS. Data sets are then registered in Hive via Spark jobs and then consumed by BI tools and platforms like Tableau. Not too mention the new environment runs on the cloud and Kubernetes. 

There are certainly pros and reasons for using an RDBMS but the requirements for this particular project could not be met with an RDBMS. There are 2 existing frameworks that will be transfered to the HDFS (top of the funnel) model. The first is a series of BTEQ scripts stored on a remote server that are scheduled through crontab. Any changes were made directly to the files in production and clearly is not a sustainable data engineering model with no GitHub integration meaning not even a continuous integration system where all the code is stored in a centralized repository with version control.

The other framework involves a slightly more sophisticated pipeline that utilizes Apache's Airflow for scheduling ETL jobs but still against an RDBMS. The jobs however, do now have continuous integration made possible with GitHub. Code changes have to be tested in a dev branch and then merged to the master branch which does not require approval. This at least keeps all the code in a central repository with version control. There is no deployment time as well which is a definite benefit here but at the cost of not having a 'demo' or 'dev' environment. The testing would be conducted locally. Airflow relies on what are called DAG files that contain all the properties and directions for the job but ultimately is a fairly simple python wrapper. 

## Now that we've discussed what the existing landscape looks like, let's talk a bit about what the future holds.
Data will be stored in HDFS in parquet format. These data will be processed by Spark for ETL jobs, ML jobs, and more. For this project, we are going to be focusing on ETL jobs. The Spark job's output is then written back to HDFS and then registered in the Hive Metastore. 

What is the Hive Metastore? 

The Hive Metastore is a metadata repository that contains a catalog of tables, schemas, file structure formats, storage locations, and table properties to provide navigation, discovery, and access to data in the Data Lake.  Compute engines use the Hive Metastore (HMS) to locate data in the Data Lake.  Additionally, HMS enables the the following:

* *Separation of compute and storage*: this de-coupling allows us to support specific technologies to optimize for specific tasks.  For example, we can utilize Trino for reporting / ad-hoc jobs and Spark for scheduled ETL.  This allows us to independently implement and scale technologies as needs change.
* *Cluster agnostic storage*: HMS’s centralized catalog provides a consolidated data access reference point.  This means that data can be read from the source cluster directly, alleviating the need for data pipelines to copy data across multiple clusters.  This reduces maintenance and complexity of data management.

<img width="1217" alt="image (19)" src="https://user-images.githubusercontent.com/20331335/120032143-97631580-bfae-11eb-8cde-cebc4fa06e14.png">

Trino can then be used to query the data in HMS. BI tools like Tableau have data connectors that allow users to connect to various data sources to use in their analaysis. Tableau has a Presto connector which can be used with Trino. The system that runs the Spark jobs is a CI/CD (Continuous Deployment) system with multiple environments that is powered by Kuberenetes. This system uses GitHub, PySpark application, .yml files and a python virtual environment. The metadata for each job is stored in a JSON file, properties for spark jobs and pipelines stored in .rml files, jobs run by starting up PySpark applications (also supports Scala), and CI/CD managed through GitHub integration and Actions. Given the volume of data and the computations used in generating the final data sets being able to store data either in memory or disk as Spark data frames and being able to scale up resources using Spark configuration properties and utilizing join capabilities like broadcasting were really big wins resulting in equivalent and in some cases better performance than the prior frameworks. The telemetry results were better than the other frameworks on top of the infrastrusture and the role of DevOps in the new tech stack.

# Walkthrough

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

Another way of doing something similar to the above is using a “runner.py” file and the below package to use subparsers so you can call Python functions from any file on the server using bash commands assuming you import the file you are parsing for the functions into the “runner.py” file.

<pre>
import os
import argparse

import file_with_functions as fl

if __name__ =='__main__':
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()

	subparsers.add_parser(‘main_func').set_defaults(func=fl.main_func)

	args = parser.parse_args()

	if 'func' in args:
		args.func()
	else:
		print("No argument is given...")
		parser.print_help()
</pre>

This really demonstrates the flexibility and how adaptable Python is to Data Engineering needs and requirements. So, what does our testing job for data availability look like? Our test case might look something like the below:

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

The next Spark job in the pipeline is going to be the main ETL job. Similarily to the first job we will import the packages required for our job. The biggest change will be within the main() function. We start by reading in a metadata reference table that is used in multiple joins later on. Because this table is referenced 10+ times in joins later in the code, it would be beneficial to cache the table in memory and broadcast the data to each worker node. In PySpark, you can use Spark SQL and the syntax would be like the below:

<pre>
spark.sql("cache table metadata_pl_df")
</pre>

This actually runs a collect() function to materialize the data frame. Using the Spark API you would do .cache() or .persist() and then perform an action on that data frame like .count().

This actually runs a collect() function to materialize the data frame. Using the Spark API you would do .cache() or .persist() and then perform an action on that data frame like .count().

<img width="1656" alt="image" src="https://user-images.githubusercontent.com/20331335/120056714-ee8ad980-bff2-11eb-810a-608f5129313a.png">
This image shows what the Spark UI looks like for this particular Spark job. You can see the In-Memory Table Scans and the Broadcast Exchanges.

The entire function is wrapped in a try/except function. This way, upon failure you can print log messages, and send custom email notifications and stop the spark session.

<pre>
except Exception as exception:
        send_failure_mail(‘email1', ‘email1', pipeline_id, pipeline_run_id, job_name, job_run_id, exception)
        print("spark_session_stop")
        spark.stop()  
        raise RuntimeError(exception)
</pre>

Because this job is designed to run periodically on a daily basis, date parameters are used throughout. Data for 8 different dates are required for each run. Because not the entire data set is required every run, we can incrementally add partitions in HDFS. Incremental, in this case still means overwriting those 8 days however. We can signficantly reduce the size of our data set by filtering for those dates and removing unnecessary records. A working table or staging table is generated with only those data required for the run. This type of staging table may be materialized if other jobs could use it as well. For this particular job, I am not materializing the staging table.

A key concept to remember in data engineering and especially when working with Spark is narrow vs wide operations. Narrow operations are operations like filtering. Operations that reduce the size of the data set. Wide operations are operations that require shuffling the data set because of aggregations for example. Counts, sum, repartition() etc… Narrow operations should always come before wide operations. Because we’ve significantly reduced the size of the underlying data set and read from a number of partitions in HDFS, we now may have an unbalanced data set or skewed data set. Because we filtered that initial data set, we may have taken 80% of the files in one partition and 20% of the files in another. This creates problems when we try to run those wide operations on this data set later on.

In order to redistribute your data equally across all partitions you would use an operation like repartition(). Coalesce() is similar although it doesn’t actually shuffle data around. The entire size of the data set is roughly 24GB and the default partition size in HDFS is 128MB so we can use repartition(200) to get the right balance to start and increase as needed for performance. Because this staging table is referenced 3 more times, we also want to cache this in memory if possible. Knowing the size of your data is critical when engineering your job as it clearly impacts key decisions in the process.

We’re using an interseting data set where you can see the movement of an element at various nodes over time. I realize thats cryptic but the takeaway here is that we now are required to perform unique counts on multiple dimensions with various conditions. This is extremely computallionally intensive. 

The data set we are working with is also designed to be a cubed data set where the measures are computed along every dimension. The values being measured are not summable. This requires repeating these unique counts over and over. In the traditional database, we were using a stored procedure to do this and it was able to run doing the counts on every different combination. Because the conditions and filters are applicable at multiple levels of detail, the plan is to instead of repeating the counts for every combination, only compute the counts for each condition and then aggregate it later using MAX or SUM, which is a much cheaper operation. 

As an example of the 3 levels of details to compute these measures over, we have Country, Global, and Rest of World. We generate the counts needed at these 3 levels of detail. We then have 15+ different measures to calculate based on those counts. Because of the repetition, we also want to cache this data frame. 

Side note - persist() will default to using in memory storage and then will spill over to disk if it has to so keep that in mind when configuring your spark job’s properties

## The final output table is structured as a presentation layer aggregate. What does that mean?
That means the data set is designed in such a way that the application using the data set can perform optimally. The data set is designed specifically for that application in a different layer than the raw data sources so other applications won’t impact performance. What that looks like in this case is there are dimensional values or cubed sets with pre-computed measures in each row. Think of it as a narrow data set and not a wide one. A wide data set might have each measure be a separate column for example.

Because of the PL structure, the Spark job has to union all 35 measures to be exact. Each measure is its own data frame. Unions however, are narrow operations and does not require a data shuffling to take place so it would have minimal impact on the job’s performance. 

## Final Steps

Repartition the final data set and write back to a new location in HDFS. Once you have written your data to HDFS, you can then register that path in the Hive Metastore catalog. Each time the job runs you are adding a new partition. That means you have to update the metadata catalog after you add a new partition in HDFS. You can run the command RECOVER PARTITIONS to accomplish this and the new partition will materialize in the HMS table. Once the HMS table has been refreshed; that step is complete and the next step is to send a signal to another database with a few stats about the run and a completion timestamp. This step is required because another application is running in a separate database that is dependent on the completion of this run. This platform is designed in such a way that you can use PySpark to read from a number of different disparate data sources.

        td.execute('''
            insert into database_one.JobRunEventLog (RunLogTs, JobName, UserId, SessionId, FinalJobStatus, ActualEndTime)
            select current_timestamp as RunLogTs, 'job_pl', USER as UserId, SESSION as SessionId, 'Completed' as FinalJobStatus, current_timestamp as ActualEndTime;
            ''')
        print('Logging Info *** DI logging ended for ', start_date)
	
<img width="1176" alt="image" src="https://user-images.githubusercontent.com/20331335/120056622-4d038800-bff2-11eb-98d9-5933924196cd.png">

# Using DevOps

The platform used to automate the spark jobs is a true CI/CD system. CI, continuous integration, is a core component of the platform. GitHub integration allows for version control and for being that centralized repositorry to store code. All code is developed and tested on dev-branches and then merged with the main branch when migrating to production. 

While working on your code, you save your changes locally and then after doing a “git pull origin main” to stay up to date, simply run [git add.; git commit -m “commit message”; git push origin dev-branch]. This commits your changes and pushes the changes to your dev branch in GitHub. 

<img width="295" alt="image" src="https://user-images.githubusercontent.com/20331335/120056565-f8600d00-bff1-11eb-8ef3-a9447e5e43df.png">

In order to merge with the main branch a Pull Request is required. This is the only approval part of this process.
<img width="1487" alt="image" src="https://user-images.githubusercontent.com/20331335/120056808-7244c600-bff3-11eb-9b53-2f36c88665c6.png">
This is a valuable and necessary step. Peer reviews are extremely valuable and this mitigates the risk of pushing bugs to production.

What is amazing about this system is that we now have 0 deployment time. We are working with a true CD system which is really nice for faster development, reliability, and performance. When changes are made to our jobs, there is no 10-15 minute wait time for the job to re-deploy to the demo environment.

What the deployment process looked like

<img width="468" alt="image" src="https://user-images.githubusercontent.com/20331335/120056859-ba63e880-bff3-11eb-8ab9-7bbac1e00976.png">

Now this is only done once and each job is de-coupled from all other jobs so the parameters and configuartions are all job agnostic.

# Conclusion

<img width="1217" alt="image (19)" src="https://user-images.githubusercontent.com/20331335/120032143-97631580-bfae-11eb-8cde-cebc4fa06e14.png">

Using new technologies, a DevOps culture, and Data Engineering excellence, a critical data pipeline consisting of over 25GB of data being stored in memory every run has been migrated off of a traditional RDBMS saving costs and resources. The jobs have been running in production since November 2020 and we've seen more reliable and performant runs. Both human and computational resouces saved. The strategy to scale this process has already been discussed and development work is already underway for more pipelines to be onboarded to this process!

## John Magrini, Master of Science in Data Science Northwestern Univirsity.
