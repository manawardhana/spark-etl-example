package com.meetup.sydney.java.spark.example

import com.meetup.sydney.java.spark.example.App

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import com.amazonaws.services.glue.util.GlueArgParser._
import com.amazonaws.services.glue.util.Job._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._


object AwsGlueEntryPoint {

  def main(sysArgs: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    val sparkContext: SparkContext = new SparkContext(conf)

    val glueContext: GlueContext = new GlueContext(sparkContext)
    val spark = glueContext.getSparkSession

    //Args
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("data-path").toArray)

    val javaArgsMap = mapAsJavaMap(args).asInstanceOf[java.util.Map[String, String]]

    //Running the jar in app
    App.runJob(spark, javaArgsMap);

  }
}
