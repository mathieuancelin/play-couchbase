package couchbase

import scala.concurrent.Future
import play.api.mvc._
import play.api.mvc.Results._
import play.api.Play.current
import com.couchbase.client.CouchbaseClient

trait CouchbaseController {

  def CouchbaseAction(block: CouchbaseClient => Future[Result]):EssentialAction = {
    Action {
      Async {
        val client = Couchbase.currentCouch(current).client.get
        block(client)
      }
    }
  }

  def CouchbaseAction(block: (play.api.mvc.Request[AnyContent], CouchbaseClient) => Future[Result]):EssentialAction = {
    Action { request =>
      Async {
        implicit val client = Couchbase.currentCouch(current).client.get
        block(request, client)
      }
    }
  }
}
