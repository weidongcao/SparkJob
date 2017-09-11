package main.scala.test

/**
  * Created by Administrator on 2017-03-22.
  */
object Sum {
  def main(args: Array[String]): Unit = {
    val arr = Array(4500, 4999, 2800, 2099, 1600, 799, 899, 499, 460, 650, 500)
    var total = 0
    for (num <- arr) {
      total += num
    }
    val str =
      """Caoweidong
   |HuXiaoqiang""".stripMargin
    println("total = " + total)

    println("Scala.Math.pow = " + math.pow(10,2))
    println("Scala.Math.log = " + math.log(9))

    println(str.stripMargin)
  }

}
