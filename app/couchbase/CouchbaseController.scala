package couchbase

import scala.concurrent.Future
import play.api.mvc._
import play.api.mvc.Results._
import play.api.Logger
import play.api.Play.current
import com.couchbase.client.CouchbaseClient

trait CouchbaseController {

  def CouchbaseAction(block: CouchbaseClient => Future[Result]):EssentialAction = {
    Action {
      Async {
        val client = Couchbase.currentCouch(current).couchbaseClient.get
        Logger.trace("Processed as non-blocking action using Async { ... }")
        block(client)
      }
    }
  }

  def CouchbaseAction(block: (play.api.mvc.Request[AnyContent], CouchbaseClient) => Future[Result]):EssentialAction = {
    Action { request =>
      Async {
        val client = Couchbase.currentCouch(current).couchbaseClient.get
        Logger.trace("Processed as non-blocking action using Async { ... }")
        block(request, client)
      }
    }
  }
}
