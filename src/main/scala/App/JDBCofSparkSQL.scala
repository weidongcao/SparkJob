package main.scala.App

import java.sql.DriverManager

/**
  * Created by Administrator on 2016/11/3.
  */
object JDBCofSparkSQL {

  def catalog =
    s"""{
        |"table":{"namespace":"default", "name":"BBS_TEST"},
        |"rowkey":"key",
        |"columns":{
        |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
        |"col1":{"cf":"cf1", "col":"col1", "type":"string"},
        |"col2":{"cf":"cf2", "col":"col2", "type":"string"},
        |"col3":{"cf":"cf3", "col":"col3", "type":"string"}
        |}
        |}""".stripMargin

  case class HBaseRecord(
                          col0: String,
                          col1: String,
                          col2: String,
                          col3: String
                        )

  object JDBCofSparkSQL {

    def main(args: Array[String]) {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      val conn = DriverManager.getConnection("jdbc:hive2://hadoop2:10000/hive", "hadoop", "")

      try {
        val statement = conn.createStatement
        val rs = statement.executeQuery("select ordernumber,amount from tbStockDetail  where amount>3000")
        while (rs.next) {
          val ordernumber = rs.getString("ordernumber")
          val amount = rs.getString("amount")
          println("ordernumber = %s, amount = %s".format(ordernumber, amount))
        }
      } catch {
        case e: Exception => e.printStackTrace
      } finally {
        conn.close
      }
    }
  }

}
