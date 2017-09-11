package main.scala.App

import java.util.UUID

import scala.io.Source

/**
  * Created by Administrator on 2016/8/23.
  */
object StringTest {


  def main(args: Array[String]): Unit = {
    /*val SESSIONID = 123

    //val model ="weiBo.set" + SESSIONID +"(doc.getFieldValue(\""+ SESSIONID +"\").asInstanceOf[String])"

    val model = "weiBo.set" + SESSIONID + "((String) doc.getFieldValue(\"" + SESSIONID + "\"));"

    print(model)

     wordcount()//调用wordcount*///.split("-")


    for (i <- 1 to 10) {
      println(UUID.randomUUID().toString.replace("-",""))
    }



  }


  def wordcount(): Unit = {
    //原始map
    val map = Source.fromFile("D:\\tmp\\csv\\sydata.txt")
      .getLines()
      .flatMap(_.split(" "))
      .foldLeft(Map.empty[String, Int]) {
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
      }

    val sortmap = map
      .toList //转成List排序
      .filter(_._2 > 1) //过滤出数量大于指定数目的数据，这里是1
      .sortWith(_._2 > _._2); //根据value值进行降序排序,( 降序（_._2 > _._2）升序(_._2 < _._2) )
    for (pair <- sortmap) println(pair) //遍历Map，输出每一个kv对
  }

}
