package com.qf.gp1922.day12

import org.apache.spark.sql.api.java.UDF1

class ConcatNameUDF extends UDF1[String, String]{
  override def call(t1: String): String = {
    "name:" + t1
  }
}
