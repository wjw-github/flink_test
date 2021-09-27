package edu.hubu

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.JdbcInputFormat
import org.apache.flink.types.Row

/*
* 访问本地MySQL数据库，读取数据表的信息
* MySQL数据库地址：localhost:3306/test_wjw  //localhost为数据库存储地址，test_wjw为数据库名称
* MySQL数据表：student,包含s_id,s_name,s_birth,s_sex 四个字段，其中s_id为主键自增长
* */
object ReadJDBCDemo {
  def main(args: Array[String]): Unit = {
    // todo: 1. 获取运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // todo: 2. Source：获取数据源
    val sourceDS: DataSet[Row] = env.createInput(JdbcInputFormat.buildJdbcInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://localhost:3306/test_wjw?useSSL=false&characterEncoding=utf8")
      .setUsername("root")
      .setPassword("wjw20011004")
      .setQuery("select s_id,s_name,s_birth,s_sex from student")
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO) //设置提取的数据对应的类型
      ).finish())

    // todo: 3. Transformation：处理数据
    val n: Long = sourceDS.count()  //统计提取的数据量（行数）

    // todo: 4. Sink：指定结果位置
    println(s"从MySQL的student表中读取出${n}条记录,内容如下：")
    sourceDS.print()

    // todo: 5. 启动执行

  }
}
