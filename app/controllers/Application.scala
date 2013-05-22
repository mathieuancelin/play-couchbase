package controllers

import play.api.mvc.{Action, Controller}
import play.api.libs.json._
import couchbase.CouchbaseReads._
import couchbase.Couchbase._
import play.api.libs.concurrent.Execution.Implicits._
import com.couchbase.client.protocol.views.{ComplexKey, Query}

//import play.api.libs.json.Reads._
//import play.api.libs.json.Writes._

case class APerson(name: String, surname: String)
case class Beer(name: String, code: String)

object Application extends Controller {

  implicit val personReader = Json.reads[APerson]
  implicit val personWriter = Json.writes[APerson]
  implicit val beerReader = new Reads[Beer] {
    def reads(json: JsValue): JsResult[Beer] = {
      val name = (json \ "name").as[String]
      val code = (json \ "brewery_id").as[String]
      JsSuccess(Beer(name, code))
    }
  }

  def index() = Action {
    Ok(views.html.index("Hello World!"))
  }

  def getContent(key: String) = Action {
    Async {
      withCouch { implicit couch =>
        getAsync[String](key).map { opt =>
          opt.map(Ok(_)).getOrElse(BadRequest(s"Unable to find content with key: $key"))
        }
      }
    }
  }

  def getPerson(key: String) = Action {
    Async {
      withCouch { implicit couch =>
        getAsync[APerson](key).map { opt =>
          opt.map(person => Ok(person.toString)).getOrElse(BadRequest(s"Unable to find person with key: $key"))
        }
      }
    }
  }

  def create() = Action {
    Async {
      val jane = APerson("Jane", "Doe")
      val json = Json.obj(
        "name" -> "Bob",
        "surname" -> "Maurane"
      )
      withCouch { implicit couch =>
        for {
          _ <- deleteAsync("bob")
          _ <- deleteAsync("jane")
          f1 <- addAsync[JsObject]("bob", -1, json)
          f2 <- addAsync[APerson]("jane", -1, jane)
        } yield Ok("bob: " +f1.getMessage + "<br/>jane: " + f2.getMessage)
      }
    }
  }

  def query() = Action {
    Async {
      withCouch { implicit couch =>
        val view = couch.getView("beer", "by_name")
        val query = new Query().setIncludeDocs(true)
          .setLimit(20)
          .setRangeStart(ComplexKey.of("(512)"))
          .setRangeEnd(ComplexKey.of("(512)" + "\uefff"))
        queryAsync[Beer](view, query).map { list =>
          Ok(list.map(_.toString).mkString("\n"))
        }
      }
    }
  }
}