package edu.hubu

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._


object JoinFunctionTest {
  def main(args: Array[String]): Unit = {

    //获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //设置程序并行度
    env.setParallelism(1)

    //创建数据源
    val students: DataSet[Student] = env
      .fromElements(Student("xiaoming","computer",90),Student("zhangmei","english",94))

    val weights: DataSet[(String,Double)] = env.fromElements(("computer",0.7),("english",0.4))

    //指定针对数据源的转换操作
    val weightedScores = students.join(weights).where(1).equalTo(0){
      (left,right) => (left.name,left.lesson,left.score*right._2)
    }

    //打印输出
    weightedScores.print()
  }

  case class Student(name: String, lesson: String, score: Int)

}
