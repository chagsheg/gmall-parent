import org.json4s.JValue
import org.json4s.jackson.JsonMethods


object Json4sDemo {
  def main(args: Array[String]): Unit = {


    val margin: String =
      """
        |{"name": "lisi","age": 20}
        |""".stripMargin

    val jvalue: JValue = JsonMethods.parse(margin)

    val jname: JValue = jvalue \ "name"
   implicit val f = org.json4s.DefaultFormats
    val str = jname.extract[String]

    val user = jvalue.extract[User]

print(user)
   // print(str)

  }
}


case class User(name:String,age:Int)