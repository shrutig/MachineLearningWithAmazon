package com.tuplejump.machinelearning

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, BinaryClassificationMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.hive.HiveContext
import io.ddf.DDFManager

object SparkS3Model {
  val credentialsConf = ConfigFactory.load("credentials").getConfig("data")
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Aws S3 application")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoopConf.set("fs.s3n.awsAccessKeyId", credentialsConf.getString("accessId"))
    hadoopConf.set("fs.s3n.awsSecretAccessKey", credentialsConf.getString("accessKey"))

    /*val hiveContext = new HiveContext(sc)
    hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, key1 INT,key2 INT,key3 INT,key4 INT,key5 INT,key6 INT," +
      "key7 INT,key8 INT,)")
    hiveContext.sql("LOAD DATA INPATH 's3n://tuplejump-source/668940643_T_ONTIME(2).csv' INTO TABLE src")

    // Queries are expressed in HiveQL
    hiveContext.sql("FROM src SELECT key, key1").collect().foreach(println)*/

    val rdd = MLUtils.loadLibSVMFile(sc,"s3n://tuplejump-source/new.txt")

    val splits = rdd.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)

    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = LogisticRegressionModel.load(sc, "myModelPath")
  }

}