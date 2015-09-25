package com.tuplejump.machinelearning

object SqlState{
  val SQL_CREATE = "CREATE TABLE airline (carrier varchar(5),flight varchar(10),origin varchar(10), dest " +
    "varchar(10),depTime int, depDelay int, taxi int, wheels int, arrival int,delay " +
    "varchar(10))"
  val SQL_COPY ="copy airline from '?' region '?' credentials " +
    "'aws_access_key_id=? ;aws_secret_access_key=?' delimiter ',' ;"
}