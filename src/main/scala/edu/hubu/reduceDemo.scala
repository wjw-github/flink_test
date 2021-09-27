package edu.hubu

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object reduceDemo {
  def main(args: Array[String]): Unit = {
    //TODO:1.创建环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //TODO:2.source数据源
    val sourceDS: DataSet[Int] = env.fromElements(1, 2, 3, 4, 5)
    val sourceDS2 = env.fromCollection(List(
      wordCount("hello", 1),
      wordCount("flink", 1),
      wordCount("flink", 1),
      wordCount("flink", 1),
      wordCount("hello", 1)
    ))

    //TODO:3.transformation数据转换操作
    val resultDS: DataSet[Int] = sourceDS.reduce(_ + _)
    val resultDS2: DataSet[wordCount] = sourceDS2.groupBy(_.word).reduce((v1, v2) => wordCount(v1.word, v1.count + v2.count))
    val resultDS3 = sourceDS2.groupBy(_.word).reduceGroup(iter => iter.reduce((v1, v2) => wordCount(v1.word, v1.count + v2.count)))
    val resultDS4 = sourceDS2.distinct(0)

    //TODO:4.sink接收器
    resultDS.print()
    println("----------")
    resultDS2.print()
    println("----------")
    resultDS3.print()
    println("----------")
    resultDS4.print()

    //TODO:5.运行
  }

  case class
  wordCount(word: String, count: Int)

}
