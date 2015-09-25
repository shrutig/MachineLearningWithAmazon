package com.tuplejump.machinelearning


import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.hive.HiveContext

object FlightSample {
  def main(args: Array[String]) {

    //Setting the logging to ERROR
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR)

    //val OutputLocation = args(0)
    val OutputLocation = "s3n://tuplejump-source"
    val conf = new SparkConf().setAppName("Flights Example")
    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration
    val credentialsConf = ConfigFactory.load("credentials").getConfig("data")
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId", credentialsConf.getString("accessId"))
    hadoopConf.set("fs.s3n.awsSecretAccessKey", credentialsConf.getString("accessKey"))
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val parquetFile = hiveContext.parquetFile("s3n://us-east-1.elasticmapreduce.samples/flightdata/input/")

    //Parquet files can also be registered as tables and then used in SQL statements.
    parquetFile.registerTempTable("flights")

    //Top 10 airports with the most departures since 2000
    val topDepartures = hiveContext.sql("SELECT deptime,  " +
      "depdelayminutes,  distancegroup, originairportseqid, destairportseqid," +
      " origincitymarketid,   airlineid,taxiout,  flightnum, distance,  flights,  " +
      " dayofweek,  wheelsoff,  destcitymarketid, depdel15,  dayofmonth,month,  year, depdelay, " +
      " quarter,arrdelay AS total_departures FROM flights WHERE year >= '2010'")
    topDepartures.rdd.saveAsTextFile(s"$OutputLocation/top_departures")

  }
}
