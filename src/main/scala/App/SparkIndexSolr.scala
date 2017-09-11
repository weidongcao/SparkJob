package main.scala.App

import java.util
import java.util.Date

import org.apache.solr.client.solrj.impl.{CloudSolrClient, HttpSolrClient}
import org.apache.solr.common.SolrInputDocument


//注册model,时间类型可以为字符串，只要后台索引配置为Long即可，注解映射形式如下


object SparkIndexSolr {

  //solr客户端
  val zkHost = "dn1.hadoop.com:2181,dn2.hadoop.com:2181,nn1.hadoop.com:2181";
  val defaultCollection = "yisou";
  val cloudBuilder = new CloudSolrClient.Builder;
  val cloudClient = cloudBuilder.withZkHost(zkHost).build();
  cloudClient.setDefaultCollection(defaultCollection);


  val client = new HttpSolrClient.Builder("http://dn1.hadoop.com:8983/solr/yisou").build();

  //批提交的条数
  val batchCount = 20;

  def main(args: Array[String]) {


    val cacheList = new util.ArrayList[SolrInputDocument]()

    for (i <- 0 to batchCount) {
      val sDoc = new SolrInputDocument();
      sDoc.addField("ID", "adadadadadad" + i)
      sDoc.addField("docType", "test")
      sDoc.addField("IMPORT_TIME", new Date())
      cacheList.add(sDoc)
    }

    //for(i<- 0 to batchCount)println(i)

    /*if (cacheList.size() > 0 || cacheList.size() == batchCount) {
      client.add(cacheList,2000)
      cacheList.clear(); //清空集合，便于重用
    }*/

    //client.commit();
    println("提交成功！")


  }


}
