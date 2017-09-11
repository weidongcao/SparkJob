package main.scala.bcpJob

import java.io.File
import java.util.UUID
import  scala.util.control.Breaks._

import org.apache.commons.io.FileUtils

import scala.io.Source

/**
  * Bcp文件工具类
  *
  * @author Cao Wei Dong
  *         create date 2017-04-07
  */
object BcpUtil {
    def main(args: Array[String]): Unit = {
        val path = convertFilContext("D:\\Program Files\\Java\\JetBrains\\workspace\\SparkJob\\src\\test")
        println(path)
    }

    /**
      * 将给定目录下所有的bcp文件进行处理
      * 将处理结果以Seq[String]类型返回
      *
      * @param path pcb文件路径
      * @return 目录下所有bcp文件的内容
      */
    def convertFilContext(path: String): String = {


        //根据给出的目录创建文件对象
        val dir = new File(path)

        //列出目录下所有符合条件的文件
        val fileList = dir.listFiles() //列出目录下所有的文件，包括目录
            .filter(_.isFile) //过滤，仅保留文件
            .filter(//过滤过滤出BCP文件
            _.toString.endsWith(".bcp") //文件对象转String,过滤出以.bcp结尾的文件
        )

        /*
         * 遍历每一个文件，读出文件内容
         */
        for (file <- fileList) {
            //读出所有文件的内容
            val context = FileUtils.readFileToString(file)
            val convertContext = context.replaceAll("\r\n", "") //去除Window下的文件换行
                .replaceAll("\n", "") //去除Linux下的文件换行
                .replaceAll("\\|\\$\\|", "\\|\\$\\|\n")

//            val tmp = new File(tmpDir.getAbsolutePath + "/" + file.getName)
//            if (tmp.exists()) {
//                tmp.delete()
//            }
//            tmp.createNewFile()
            FileUtils.writeStringToFile(file, convertContext)
        }

        //返回结果
        path
    }
}
