package com.atguigu.gmall.gmallpublisher.service

object DSL {
  def getDauDSL(): String =
    """
      |{
      |  "query": {
      |    "match_all": {}
      |  }
      |}
      |""".stripMargin

  def getHourDauDSL() = {
    """
      |{
      |  "query": {
      |    "match_all": {}
      |  },
      |  "aggs": {
      |    "group_by_hour": {
      |      "terms": {
      |        "field": "logHour",
      |        "size": 24
      |      }
      |    }
      |  }
      |}
      |""".stripMargin
  }
}
