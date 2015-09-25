package com.tuplejump.machinelearning

import java.io.File

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.machinelearning.AmazonMachineLearningClient
import com.amazonaws.services.machinelearning.model._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.PutObjectRequest
import scala.io.Source
import com.typesafe.config.{Config, ConfigFactory}

object AwsModel {
  val PREFIX = "Scala aws ml"
  val configCredentials: Config = ConfigFactory.load("credentials").getConfig("data")
  val credentials = new BasicAWSCredentials(configCredentials.getString("accessId"),
   configCredentials.getString("accessKey"))
  val client = new AmazonMachineLearningClient(credentials)
  //val id = createDataSourceFromS3("data1","s3://tuplejump-source/668940643_T_ONTIME.csv","/banking.csv.schema",1, 2)
  //println(id)
  //val id2 =  AwsModel.createModel("ds-ICR3x1CRmru","/recipe.json",MLModelType.MULTICLASS)
  //val id3= AwsModel.createEvaluation("ml-h3szsBObg4O","ds-SRZjI0EborT")
  //val id4 = AwsModel.createDataSourceFromRedShift("database","dataR2","/airlineBatch.csv.schema",1,2)
  //RedshiftDB.createTable()
  //val id5 = AwsModel.createBatchPrediction("ml-ESr1yNk6fID","ds-kWUaoiWwRGs", "s3://tuplejump-source-2/")

  val JDBC_NAME = "jdbcName"
  val CLUSTER_NAME = "clusterName"
  val DB_USER = "dbUser"
  val DB_PASS = "dbPass"
  val ML_STAGE = "mlStage"
  val ML_ROLE_ARN = "mlRoleArn"
  val SQL_QUERY ="sqlQuery"

  def createDataSourceFromS3(entityName: String,dataUrl:String,dataSchema:String, percentBegin: Int,
  percentEnd: Int):String
    = {
    val entityId = Identifiers.newDataSourceId
    val dataRearrangementString = """{"splitting":{"percentBegin":""" + percentBegin + ""","percentEnd":""" +
      percentEnd + """}}"""
    val dataSpec = new S3DataSpec()
      .withDataLocationS3(dataUrl)
      .withDataRearrangement(dataRearrangementString)
      .withDataSchema(Source.fromInputStream(getClass.getResourceAsStream(dataSchema)).mkString)
    val request = new CreateDataSourceFromS3Request()
      .withDataSourceId(entityId)
      .withDataSourceName(entityName)
      .withComputeStatistics(true)
      .withDataSpec(dataSpec)

    client.createDataSourceFromS3(request)
    println(s"Created DataSource S3 : $entityName with id $entityId")
    entityId

  }

  def uploadToS3(filePath:String , fileId: String,bucketName:String) {
    val s3Client = new AmazonS3Client(credentials)
    val  file = new File(filePath)
    val objectRequest = new PutObjectRequest(bucketName, fileId, file)
    s3Client.putObject(objectRequest)
  }

  def createDataSourceFromRedShift(fileName:String, entityName: String,dataSchema:String,
                                   percentBegin:
    Int, percentEnd: Int): String = {
    val entityId = Identifiers.newDataSourceId
    val dataRearrangementString = """{"splitting":{"percentBegin":""" + percentBegin + ""","percentEnd":""" +
      percentEnd + """}}"""
    val config: Config = ConfigFactory.load(fileName).getConfig("data")
    val database = new RedshiftDatabase()
      .withDatabaseName(config.getString(JDBC_NAME))
      .withClusterIdentifier(config.getString(CLUSTER_NAME))
    val databaseCredentials = new RedshiftDatabaseCredentials()
      .withUsername(config.getString(DB_USER))
      .withPassword(config.getString(DB_PASS))
    val dataSpec = new RedshiftDataSpec()
      .withDatabaseInformation(database)
      .withDatabaseCredentials(databaseCredentials)
      .withSelectSqlQuery(config.getString(SQL_QUERY))
      .withDataRearrangement(dataRearrangementString)
      .withDataSchema(Source.fromInputStream(getClass.getResourceAsStream(dataSchema)).mkString)
      .withS3StagingLocation(config.getString(ML_STAGE))
    val request = new CreateDataSourceFromRedshiftRequest()
      .withComputeStatistics(false)
      .withDataSourceId(entityId)
      .withDataSpec(dataSpec)
      .withRoleARN(config.getString(ML_ROLE_ARN))
      .withDataSourceName(entityName)
    client.createDataSourceFromRedshift(request)
    println(s"Created DataSource from Redshift : $entityName with id $entityId")
    entityId
  }

  def createModel(trainDataSourceId: String,recipe:String,modelType:MLModelType): String = {
    val mlModelId = Identifiers.newMLModelId
    val request = new CreateMLModelRequest()
      .withMLModelId(mlModelId)
      .withMLModelName(PREFIX + " model")
      .withMLModelType(modelType)
      .withRecipe(Source.fromInputStream(getClass.getResourceAsStream(recipe)).mkString)
      .withTrainingDataSourceId(trainDataSourceId)
    client.createMLModel(request)
    println(s"Created ML Model with id $mlModelId")
    mlModelId
  }

  def createEvaluation(mlModelId: String,testDataSourceId:String): String = {
    val evaluationId = Identifiers.newEvaluationId
    val request = new CreateEvaluationRequest()
      .withEvaluationDataSourceId(testDataSourceId)
      .withEvaluationId(evaluationId)
      .withEvaluationName(PREFIX + " evaluation")
      .withMLModelId(mlModelId)
    client.createEvaluation(request)
    println(s"Created Evaluation with id $evaluationId")
    evaluationId
  }

  def createBatchPrediction(mlModelId: String,dataSourceId:String, s3OutputUrl: String): String = {
    val batchPredictionId = Identifiers.newBatchPredictionId
    val bpRequest = new CreateBatchPredictionRequest()
      .withBatchPredictionId(batchPredictionId)
      .withBatchPredictionName("Batch Prediction")
      .withMLModelId(mlModelId)
      .withOutputUri(s3OutputUrl)
      .withBatchPredictionDataSourceId(dataSourceId)
    client.createBatchPrediction(bpRequest)
    println(s"Created BatchPrediction with id $batchPredictionId")
    batchPredictionId
  }
  
}
