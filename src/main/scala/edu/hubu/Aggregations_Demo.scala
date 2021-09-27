package edu.hubu

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object Aggregations_Demo {
  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //创建数据源
    val input: DataSet[(Int,String,Double)] = env.fromElements(
      (1,"flink",5.0),
      (1,"spark",3.0),
      (1,"spark",4.0),
      (1,"spark",4.0),
      (1,"flink",6.0)
    )

    //指定针对数据集的转换操作
    val output: DataSet[(Int,String,Double)] = input
      .aggregate(Aggregations.SUM,0)
      .and(Aggregations.MAX,2)

    val output2: DataSet[(Int,String,Double)] = input
      .groupBy(1)
      .aggregate(Aggregations.SUM,0)
      .and(Aggregations.MIN,2)

    //打印输出
    output.print()
    println("--------")
    output2.print()
  }

}
