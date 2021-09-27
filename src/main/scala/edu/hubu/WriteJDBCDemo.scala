package edu.hubu

import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.JdbcOutputFormat
import org.apache.flink.types.Row

import scala.collection.mutable.ArrayBuffer

/*
* 访问本地MySQL数据库，写入2条记录到student数据表
* MySQL数据库地址：localhost:3306/test_wjw  //localhost为数据库存储地址，test_wjw为数据库名称
* MySQL数据表：student,包含s_id,s_name,s_birth,s_sex 四个字段，其中s_id为主键自增长
* */
object WriteJDBCDemo {
  def main(args: Array[String]): Unit = {
    // todo: 1. 获取运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // todo: 2. Source：获取数据源
    val arr: ArrayBuffer[Row] = new ArrayBuffer[Row]()

    val row1: Row = new Row(4)
    row1.setField(0,"09")
    row1.setField(1,"张山")
    row1.setField(2,"2000-01-01")
    row1.setField(3,"男")

    val row2: Row = new Row(4)
    row2.setField(0,"10")
    row2.setField(1,"丁水")
    row2.setField(2,"2020-11-11")
    row2.setField(3,"女")

    arr.+=(row1)
    arr.+=(row2)

    val dataDS: DataSet[Row] = env.fromCollection(arr)

    // todo: 3. Transformation：处理数据

    // todo: 4. Sink：指定结果位置
    dataDS.output(JdbcOutputFormat.buildJdbcOutputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://localhost:3306/test_wjw?useSSL=false&characterEncoding=utf8")
      .setUsername("root")
      .setPassword("wjw20011004")
      .setQuery("insert into student(s_id,s_name,s_birth,s_sex) values(?,?,?,?)")
      .finish())

    // todo: 5. 启动执行
    env.execute("insert data to mysql")
    println("写入成功")
  }
}
