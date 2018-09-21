import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import spray.json._

import scala.concurrent.Future
import scala.util.{Failure, Success}

case class ContServResp(currentTime: String, controllerServices: ContServ)
case class ContServ(revision: Revision, id: String, uri: String,component: Component)
case class Revision(clientId: String, version: String)
case class Component(id: String, name: String, state: String, refComp: RefComp)
case class RefComp(revision: Revision, id: String, name: String, state: String)

trait Protocols extends DefaultJsonProtocol {

  implicit val contServRespFormat: RootJsonFormat[ContServResp] = jsonFormat(ContServResp.apply, "currentTime", "controllerServices")
  implicit val contServFormat: RootJsonFormat[ContServ] = jsonFormat(ContServ.apply, "revision", "id", "uri", "component")
  implicit val revFormat: RootJsonFormat[Revision] = jsonFormat(Revision.apply, "clientId", "version")
  implicit val compFormat: RootJsonFormat[Component] = jsonFormat(Component.apply, "id", "name", "state", "refComp")
  implicit val refCompFormat: RootJsonFormat[RefComp] = jsonFormat(RefComp.apply, "revision", "id", "name", "state")


//  implicit val contServRespFormat = jsonFormat2(ContServResp.apply)

}

object handler extends App with Protocols{

  val baseUrl = Uri("http://10.9.0.46:9091/nifi-api/flow/process-groups/b9758447-0164-1000-57a8-4477ce3943d4/controller-services")
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  //val api_key =  // your api key here

  /*
  val query: Query = Query("api_key" -> api_key,
    "sensor_name" -> "N05171T",
    "start_time" -> "20170201",
    "end_time" -> "20170202",
    "variable" -> "average speed")
*/

  val url = "http://10.9.0.46:9091/nifi-api/flow/process-groups/b9758447-0164-1000-57a8-4477ce3943d4/controller-services"
  val res1 = scala.io.Source.fromURL(url).mkString.parseJson

  val converted = res1.convertTo[ContServResp]

  //println(ContServResp)

  val res: Future[HttpResponse] = Http().singleRequest(HttpRequest(HttpMethods.GET, uri = baseUrl))//uri.withQuery(query)))

  res andThen {
    case Success(response) => {
      response.
        entity.
        dataBytes.
        map(_.utf8String).
        map(_.parseJson.convertTo[List[ContServResp]]).
        runForeach(println)
    }
    case Failure(ex) => println(ex)
  } onComplete {
    _ => system.terminate()
  }

}
