import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.nifi.action.Component
import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.Json
import org.json4s.jackson.JsonMethods._



/**
  * Created by sild004 on 2018-09-12.
  */

object nifiHandler extends App {
  try{


    val compName = ""

    val compId = ""
    val contServUri = ("{}flow/process-groups/{}/controller-services", nifiUrl, compId)
    val compName = ""

    val url = "http://10.9.0.46:9091/nifi-api/flow/process-groups/b9758447-0164-1000-57a8-4477ce3943d4"

    def createUri(uri: String, id: String, kind: String ): String = {
      val nifiUrl = "http://10.9.0.46:9091/nifi-api/"
      s"${uri}flow/process-groups/${id}/${kind}"
    }

    val jsonRaw = parse(getRequest(url))

    val revision = (jsonRaw \\ "controllerServices" \\ "revision").children
    val component = (jsonRaw \\ "controllerServices" \\ "component").children
    val compServices = (jsonRaw \\ "controllerServices").children
    //val component = (jsonRaw \\ "controllerServices" \\ "component" \\ "referencingComponents").children

    var idComp, nameComp, endComp, idClient, nameClient = ""
    val filterComp = "HiveConnectionPool"

    for {
      JObject(iterComp) <- compServices
      JField("id", JString(id))  <- iterComp
      //JField("name", JString(name))  <- iterComp
      JField("uri", JString(uri))  <- iterComp
      //if name == filterComp
    } yield (idComp = id, nameComp = name, endComp = uri)
    } yield print((id, uri))

    for {
      JObject(iterRev) <- revision
      JField("clientId", JString(clientId))  <- iterRev
      JField("version", JString(version))  <- iterRev
    } yield (clientId, version)


    //println(idComp, nameComp, stateComp)
    //print(component)

/*
    for (serv <- component) {
      println(s"${serv.name}, ${serv.id}, ${serv.version}")
    }

    val jsonSession = createSparkSession("Json Reader", "local")
    val df = jsonSession.read.json(getRequest(url))
    df.show
    */



  } catch {

    case unknown: Exception =>
      println(s"Unknown exception: $unknown")

  }

  def getRequest(url: String): String = {
    scala.io.Source.fromURL(url).mkString
  }

  def createSparkSession(appName: String, master: String) = {
      SparkSession
      .builder()
      .master(master)
      .appName(appName)
      .getOrCreate
  }
}


