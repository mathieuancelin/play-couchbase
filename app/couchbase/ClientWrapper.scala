package couchbase
import play.api.libs.json._
import com.couchbase.client.CouchbaseClient
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import com.couchbase.client.protocol.views.{Query, View}
import collection.JavaConversions._

// Yeah I know JavaFuture.get is really ugly, but what can I do ???
trait ClientWrapper {

  def find[T](view: View, query: Query)(implicit client: CouchbaseClient, r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    Future {
      val results = client.asyncQuery(view, query).get
      results.iterator().map { result =>
        r.reads(Json.parse(result.getDocument.asInstanceOf[String])) match {
          case e:JsError => {println(e.toString);None}
          case s:JsSuccess[T] => s.asOpt
        }
      }.toList.filter(_.isDefined).map(_.get)
    }(ec)
  }

  def get[T](key: String)(implicit client: CouchbaseClient, r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    Future {
      client.asyncGet(key).get() match {
        case value: String => r.reads(Json.parse(value)).asOpt
        case _ => None
      }
    }(ec)
  }

  def set[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.set(key, exp, Json.stringify(w.writes(value)))
      future.get
      future.getStatus
    }(ec)
  }

  def add[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.add(key, exp, Json.stringify(w.writes(value)))
      future.get
      future.getStatus
    }(ec)
  }

  def delete(key: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.delete(key)
      future.get
      future.getStatus
    }(ec)
  }

  def replace[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.replace(key, exp, Json.stringify(w.writes(value)))
      future.get
      future.getStatus
    }(ec)
  }

  def set[T](key: String, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, 0, value)(client, w, ec)
  }

  def add[T](key: String, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, 0, value)(client, w, ec)
  }

  def replace[T](key: String, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, 0, value)(client, w, ec)
  }
}
