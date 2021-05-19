package com.atguigu.gmall.realtime.util

import java.sql.{DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

object JDBCUtil {

  /**
   * 执行 sql, 并把查询到的结果封装到 C 类中
   */
  def query(url: String, sql: String, args: List[Object]): List[Map[String, Object]] = {
    var result = List[Map[String, Object]]()

    val conn = DriverManager.getConnection(url)
    val ps: PreparedStatement = conn.prepareStatement(sql)
    (1 to args.size).foreach(i => {
      ps.setObject(i, args(i - 1))
    })
    val resultSet: ResultSet = ps.executeQuery()
    val meta: ResultSetMetaData = resultSet.getMetaData // 需要知道有多少列
    while (resultSet.next()) {
      var map: Map[String, Object] = Map[String, Object]()
      for (i <- 1 to meta.getColumnCount) { // 遍历每一列
        val name = meta.getColumnName(i).toLowerCase()
        val value = resultSet.getObject(i)
        map += name -> value
      }
      result :+= map
    }
    ps.close()
    conn.close()
    result
  }
}