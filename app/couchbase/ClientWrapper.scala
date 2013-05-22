package couchbase
import play.api.libs.json._
import com.couchbase.client.CouchbaseClient
import scala.concurrent.{Promise, Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import com.couchbase.client.protocol.views.{Query, View}
import collection.JavaConversions._
import akka.actor.ActorSystem
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import net.spy.memcached.internal.OperationFuture
import java.util.concurrent.atomic.AtomicBoolean

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
    //ClientWrapperPoller.promise(client.set(key, exp, Json.stringify(w.writes(value))))
    Future {
      val future = client.set(key, exp, Json.stringify(w.writes(value)))
      future.get
      future.getStatus
    }(ec)
  }

  def add[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    //ClientWrapperPoller.promise(client.add(key, exp, Json.stringify(w.writes(value))))
    Future {
      val future = client.add(key, exp, Json.stringify(w.writes(value)))
      future.get
      future.getStatus
    }(ec)
  }

  def delete(key: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    //ClientWrapperPoller.promise(client.delete(key))
    Future {
      val future = client.delete(key)
      future.get
      future.getStatus
    }(ec)
  }

  def replace[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    //ClientWrapperPoller.promise(client.replace(key, exp, Json.stringify(w.writes(value))))
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

case class Check(scalaFuture: Promise[OperationStatus], javaFuture: OperationFuture[java.lang.Boolean], done: AtomicBoolean)
object ClientWrapperPoller {
  val system = ActorSystem("JavaFutureToScalaFuture")
  var waiting = List[Check]()
  val queue = new ConcurrentLinkedQueue[Check]()
  import play.api.libs.concurrent.Execution.Implicits._
  system.scheduler.schedule(FiniteDuration(0, TimeUnit.MILLISECONDS), FiniteDuration(10, TimeUnit.MILLISECONDS)) {
    waiting = queue.toList ++: waiting
    queue.clear()
    waiting.foreach { check =>
       if (check.javaFuture.isDone) {
         if (check.javaFuture.getStatus.isSuccess) {
           check.scalaFuture.success(check.javaFuture.getStatus)
         } else {
           check.scalaFuture.failure(new Throwable(check.javaFuture.getStatus.getMessage))
         }
         check.done.set(true)
       }
    }
    waiting = waiting.filter(!_.done.get())
  }
  def promise(javaFuture: OperationFuture[java.lang.Boolean]): Future[OperationStatus] = {
    val p = Promise[OperationStatus]()
    queue.offer(Check(p, javaFuture, new AtomicBoolean(false)))
    p.future
  }
}
