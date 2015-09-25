package com.tuplejump.machinelearning

import com.amazon._
import java.sql._

import com.typesafe.config.{ConfigFactory, Config}
import com.zaxxer.hikari.HikariDataSource

object RedshiftDB {
  private var dataSource: HikariDataSource = null

  def initializeDataSource(fileName: String) = {
    try {
      Class.forName("com.amazon.redshift.jdbc41.Driver")
      val config: Config = ConfigFactory.load(fileName).getConfig("data")
      dataSource = new HikariDataSource()
      dataSource.setJdbcUrl(config.getString("dbUrl"))
      dataSource.setUsername(config.getString("dbUser"))
      dataSource.setPassword(config.getString("dbPass"))
    }
    catch {
      case e: Exception => {
        throw new Exception("initialize data source failed", e)
      }
    }
  }

  def updateSql(sql: String, dbFile: String) {
    if (dataSource == null)
      initializeDataSource(dbFile)
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = dataSource.getConnection
      preparedStatement = connection.prepareStatement(sql)
      preparedStatement.executeUpdate()
    }
    catch {
      case e: Exception => {
        throw new Exception("update sql failed", e)
      }
    }
    finally {
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }
  }

  def executeSql(sql: String, dbFile: String): ResultSet = {
    if (dataSource == null)
      initializeDataSource(dbFile)
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      connection = dataSource.getConnection
      preparedStatement = connection.prepareStatement(sql)
      val rs = preparedStatement.executeQuery()
      rs
    }
    catch {
      case e: Exception => {
        throw new Exception("execute sql failed", e)
      }
    }
    finally {
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }
  }

  def copyFromS3(s3Source: String, credentialsFile: String, region: String, dbFile: String) = {
    if (dataSource == null)
      initializeDataSource(dbFile)
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try {
      val config: Config = ConfigFactory.load(credentialsFile).getConfig("data")
      connection = dataSource.getConnection
      preparedStatement = connection.prepareStatement(SqlState.SQL_COPY)
      preparedStatement.setString(1, s3Source)
      preparedStatement.setString(2, region)
      preparedStatement.setString(3, region)
      preparedStatement.setString(4, config.getString("accessId"))
      preparedStatement.setString(5, config.getString("accessKey"))
      preparedStatement.executeUpdate()
    }
    catch {
      case e: Exception => {
        throw new Exception("copy from S3 failed", e)
      }
    }
    finally {
      if (preparedStatement != null)
        preparedStatement.close()
      if (connection != null)
        connection.close()
    }
  }


}