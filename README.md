# Sydney Java Meetup #17 - Data Processing at Scale using Apache Spark and Java

## Introduction

This project contains the simple example Jave project used in the meetup talk and the Scala script that does a similar data processing.
While Apache Spark contains different modules like GraphX and MLib, this only covers foundation capability of batch data processing. 

## Sample Input Data
The data set can be found on:
https://www.kaggle.com/datasets/pypiahmad/goodreads-book-reviews1/data

Only the following files have been used for this example:
* goodreads_book_authors.json
* goodreads_book_genres_initial.json
* goodreads_books.json

## Environment Setup
* Install JDK 1.8
* Install Maven 3.8.x (tested version)
* Download and extract Apache Spark 3.3.2
 * https://spark.apache.org/downloads.html
* Clone this project
* Download and copy the relevant 3 goodreads-book-review data files as follows:

```
➜  java-meetup-spark tree -L 4
.
├── README.md
├── data
│   └── good_read_books
│       ├── goodreads_book_authors.json                     <- Placed downloaded file
│       ├── goodreads_book_genres_initial.json              <- Placed downlaoded file
│       ├── goodreads_books.json                            <- Placed downlaoded file
│       └── parquet                                         \ 
│           ├── goodreads_book_authors.parquet              | 
│           ├── goodreads_book_genres_initial.parquet       }  Script will convert JSON data files into these parquet files
│           ├── goodreads_books.parquet                     |
│           └── partitioned                                 / 
├── good-read.scala                                         <- Scala script that can be run on Spark Shell
├── pom.xml                                                 <- Maven build config for the Java Project
└── src                                                     <- Java Source
    ├── main
    │   └── java
    └── test
        └── java
```

* Set the following environmental variables as appropriate:

``` sh
export SPARK_HOME="{spark_home_dir_goes_here}";
export PROJECT_HOME="{project_home_dir_goes_here}";
```

## Running the Project

### Running the script on Spark Shell

``` sh
cd ${PROJECT_HOME};
# deleting any output result files previously generated
rm -rf data/good_read_books/parquet/partitioned/books_result.parquet; rm -rf data/good_read_books/parquet/books_result.parquet; # If exists. Regarding subsequent executions...
# executing via spark-shell
${SPARK_HOME}/bin/spark-shell --conf "spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED" --driver-memory=4G -i "${PROJECT_HOME}/good-read.scala";
```
#### Quiting Spark Shell

```
spark> :quit
```
### Running the java project

``` sh
mvn clean install;
~/bin/spark-3.3.2-bin-hadoop3/bin/spark-submit --class com.meetup.sydney.java.spark.example.App --master local target/spark-example-java-1.0-SNAPSHOT.jar;
```

## Monitoring
To monitor the spark job, access http://localhost:4040/ and freely navigate the UI __while the job is executing__.

## Running on AWS Glue ETL
Apache Spark Jobs can run on AWS Glue ETL Jobs with a very little customizations.

AWS Glue doesn't directly support Java language. But there is a workaround for this. You can compile your Java Spark project and call it by a tiny Scala Glue script.
This project has necessary customizations to do that (including Glue related dependencies in pom.xml).

### Prerequisites:
* AWS CLI V2
* Create the AWS IAM role with relevant permission as appropriate. Typically this involves permission to run 

### Creating AWS Glue Job
``` sh
export GLUE_JOB_NAME="java-meetup-spark";
export GLUE_JOB_IAM_ROLE="{{YOUR-IAM-ROLE-NAME-GOES-HERE}}";
export DATA_PATH="s3://{{YOUR-S3-BUCKET-NAME}}/good_read_books";
export SCRIPT_PATH="s3://{{YOUR-S3-BUCKET-NAME}}/AwsGlueEntryPoint.scala";
export EXTRA_JAR_PATH="s3://{{YOUR-S3-BUCKET-NAME}}/spark-example-java-1.0-SNAPSHOT.jar";
export CLASS_NAME="com.meetup.sydney.java.spark.example.AwsGlueEntryPoint";
export AWS_REGION="ap-southeast-2";

# shell command
aws glue create-job \
    --name "${GLUE_JOB_NAME}" \
    --role "${GLUE_JOB_IAM_ROLE}" \
    --command "{ \
        \"Name\": \"glueetl\", \
        \"ScriptLocation\": \"${SCRIPT_PATH}\" \
    }" \
    --region "${AWS_REGION}" \
    --glue-version "4.0" \
    --default-arguments "{ \
        \"--job-language\":\"scala\", \
        \"--extra-jars\":\"${EXTRA_JAR_PATH}\", \
        \"--class\":\"${CLASS_NAME}\" \
    }"
```

### Running the AWS Glue Job
``` sh
# shell command
aws glue start-job-run \
  --region "${AWS_REGION}" \
  --job-name "${GLUE_JOB_NAME}" \
  --arguments  "{\"--data-path\":\"${DATA_PATH}\"}"
```
