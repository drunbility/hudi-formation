package org.example

import cn.hutool.http.HttpUtil
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConversions._

class queryCompleteListener(appName: String) extends SparkListener with Logging {


  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {


    val query = new Query()
    query.setId(appName)
    query.setStatus("ended")

    val mapper = new ObjectMapper()


    val str = mapper.writeValueAsString(query)



    logWarning(HttpUtil.post("http://10.0.208.xxx:7070/api/v1/querys", str))


  }


}
