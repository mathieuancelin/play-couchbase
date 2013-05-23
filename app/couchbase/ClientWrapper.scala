package couchbase
import play.api.libs.json._
import com.couchbase.client.CouchbaseClient
import scala.concurrent.{Promise, Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import com.couchbase.client.protocol.views.{SpatialView, DesignDocument, Query, View}
import collection.JavaConversions._
import net.spy.memcached.{ReplicateTo, PersistTo}
import akka.actor.ActorSystem
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import net.spy.memcached.internal.OperationFuture
import com.couchbase.client.internal.HttpFuture
import play.api.Play.current

// Yeah I know JavaFuture.get is really ugly, but what can I do ???
// http://stackoverflow.com/questions/11529145/how-do-i-wrap-a-java-util-concurrent-future-in-an-akka-future
trait ClientWrapper {

  def find[T](view: View, query: Query)(implicit client: CouchbaseClient, r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    wrapJavaFutureInPureFuture( client.asyncQuery(view, query), ec ).map { results =>
      results.iterator().map { result =>
        r.reads(Json.parse(result.getDocument.asInstanceOf[String])) match {
          case e:JsError => {println(e.toString);None}
          case s:JsSuccess[T] => s.asOpt
        }
      }.toList.filter(_.isDefined).map(_.get)
    }
//    Future {
//      val results = client.asyncQuery(view, query).get
//      results.iterator().map { result =>
//        r.reads(Json.parse(result.getDocument.asInstanceOf[String])) match {
//          case e:JsError => {println(e.toString);None}
//          case s:JsSuccess[T] => s.asOpt
//        }
//      }.toList.filter(_.isDefined).map(_.get)
//    }(ec)
  }

  def view(docName: String, viewName: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[View] = {
    wrapJavaFutureInPureFuture( client.asyncGetView(docName, viewName), ec )
//    Future {
//      client.asyncGetView(docName, viewName).get()
//    }(ec)
  }

  def spatialView(docName: String, viewName: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[SpatialView] = {
    wrapJavaFutureInPureFuture( client.asyncGetSpatialView(docName, viewName), ec )
//    Future {
//      client.asyncGetSpatialView(docName, viewName).get()
//    }(ec)
  }

  /** def designDocument(docName: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[DesignDocument] = {
    Future {
      client.asyncGetDesignDocument(docName).get.asInstanceOf[DesignDocument]
    }(ec)
  }  **/

  def createDesignDoc(name: String, value: JsObject)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.asyncCreateDesignDoc(name, Json.stringify(value)), ec )
//    Future {
//      client.asyncCreateDesignDoc(name, Json.stringify(value)).getStatus
//    }(ec)
  }

  def createDesignDoc(name: String, value: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.asyncCreateDesignDoc(name, value), ec )
//    Future {
//      client.asyncCreateDesignDoc(name, value).getStatus
//    }(ec)
  }

  def createDesignDoc(value: DesignDocument[_])(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.asyncCreateDesignDoc(value), ec )
//    Future {
//      client.asyncCreateDesignDoc(value).getStatus
//    }(ec)
  }

  def deleteDesignDoc(name: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.asyncDeleteDesignDoc(name), ec )
//    Future {
//      client.asyncDeleteDesignDoc(name).getStatus
//    }(ec)
  }

  def keyStats(key: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[Map[String, String]] = {
    wrapJavaFutureInPureFuture( client.getKeyStats(key), ec ).map(_.toMap)
//    Future {
//       client.getKeyStats(key).get().toMap
//    }(ec)
  }

  def get[T](key: String)(implicit client: CouchbaseClient, r: Reads[T], ec: ExecutionContext): Future[Option[T]] = {
    wrapJavaFutureInPureFuture( client.asyncGet(key), ec ).map { f =>
       f match {
         case value: String => r.reads(Json.parse(value)).asOpt
         case _ => None
       }
    }
//    Future {
//      client.asyncGet(key).get() match {
//        case value: String => r.reads(Json.parse(value)).asOpt
//        case _ => None
//      }
//    }(ec)
  }

  def set[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.set(key, exp, Json.stringify(w.writes(value))), ec )
//    Future {
//      val future = client.set(key, exp, Json.stringify(w.writes(value)))
//      future.get
//      future.getStatus
//    }(ec)
  }

  def set[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.set(key, exp, value, replicateTo), ec )
//    Future {
//      val future = client.set(key, exp, value, replicateTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def set[T](key: String, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, 0, value, replicateTo)(client, w, ec)
  }
  def set[T](key: String, exp: Int, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.set(key, exp, value, peristTo), ec )
//    Future {
//      val future = client.set(key, exp, value, peristTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def set[T](key: String, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, 0, value, peristTo)(client, w, ec)
  }
  def set[T](key: String, exp: Int, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.set(key, exp, value, peristTo, replicateTo), ec )
//    Future {
//      val future = client.set(key, exp, value, peristTo, replicateTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def set[T](key: String, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, 0, value, peristTo, replicateTo)(client, w, ec)
  }

  def add[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.add(key, exp, Json.stringify(w.writes(value))), ec )
//    Future {
//      val future = client.add(key, exp, Json.stringify(w.writes(value)))
//      future.get
//      future.getStatus
//    }(ec)
  }

  def add[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.add(key, exp, value, replicateTo), ec )
//    Future {
//      val future = client.add(key, exp, value, replicateTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def add[T](key: String, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, 0, value, replicateTo)(client, w, ec)
  }
  def add[T](key: String, exp: Int, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.add(key, exp, value, peristTo), ec )
//    Future {
//      val future = client.add(key, exp, value, peristTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def add[T](key: String, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, 0, value, peristTo)(client, w, ec)
  }
  def add[T](key: String, exp: Int, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.add(key, exp, value, peristTo, replicateTo), ec )
//    Future {
//      val future = client.add(key, exp, value, peristTo, replicateTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def add[T](key: String, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, 0, value, peristTo, replicateTo)(client, w, ec)
  }

  def delete(key: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.delete(key), ec )
//    Future {
//      val future = client.delete(key)
//      future.get
//      future.getStatus
//    }(ec)
  }

  def delete[T](key: String, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.delete(key, replicateTo), ec )
//    Future {
//      val future = client.delete(key, replicateTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def delete[T](key: String, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.delete(key, peristTo), ec )
//    Future {
//      val future = client.delete(key, peristTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def delete[T](key: String, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.delete(key, peristTo, replicateTo), ec )
//    Future {
//      val future = client.delete(key, peristTo, replicateTo)
//        future.get
//      future.getStatus
//    }(ec)
  }

  def replace[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.replace(key, exp, Json.stringify(w.writes(value))), ec )
//    Future {
//      val future = client.replace(key, exp, Json.stringify(w.writes(value)))
//      future.get
//      future.getStatus
//    }(ec)
  }

  def replace[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.replace(key, exp, value, replicateTo), ec )
//    Future {
//      val future = client.replace(key, exp, value, replicateTo)
//        future.get
//      future.getStatus
//    }(ec)
  }
  def replace[T](key: String, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
     replace(key, 0, value, replicateTo)(client, w, ec)
  }
  def replace[T](key: String, exp: Int, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.replace(key, exp, value, peristTo), ec )
//    Future {
//      val future = client.replace(key, exp, value, peristTo)
//      future.get
//      future.getStatus
//    }(ec)
  }
  def replace[T](key: String, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, 0, value, peristTo)(client, w, ec)
  }
  def replace[T](key: String, exp: Int, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.replace(key, exp, value, peristTo, replicateTo), ec )
//    Future {
//      val future = client.replace(key, exp, value, peristTo, replicateTo)
//      future.get
//      future.getStatus
//    }(ec)
  }
  def replace[T](key: String, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, 0, value, peristTo,replicateTo)(client, w, ec)
  }

  def flush(delay: Int)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    wrapJavaFutureInFuture( client.flush(delay), ec )
//    Future {
//      val future = client.flush(delay)
//      future.get
//      future.getStatus
//    }(ec)
  }

  def flush()(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
     flush(0)(client, ec)
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

  private def wrapJavaFutureInPureFuture[T](javaFuture: java.util.concurrent.Future[T], ec: ExecutionContext): Future[T] = {
    if (Polling.pollingFutures) {
      val promise = Promise[T]()
      pollJavaFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      promise.future
    } else {
       Future {
         javaFuture.get
       }(ec)
    }
  }

  private def wrapJavaFutureInFuture[T](javaFuture: OperationFuture[T], ec: ExecutionContext): Future[OperationStatus] = {
    if (Polling.pollingFutures) {
      val promise = Promise[OperationStatus]()
      pollCouchbaseFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      promise.future
    } else {
      Future {
        javaFuture.get
        javaFuture.getStatus
      }(ec)
    }
  }

  private def wrapJavaFutureInFuture[T](javaFuture: HttpFuture[T], ec: ExecutionContext): Future[OperationStatus] = {
    if (Polling.pollingFutures) {
      val promise = Promise[OperationStatus]()
      pollCouchbaseFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      promise.future
    } else {
      Future {
        javaFuture.get
        javaFuture.getStatus
      }(ec)
    }
  }

  private def pollJavaFutureUntilDoneOrCancelled[T](javaFuture: java.util.concurrent.Future[T], promise: Promise[T], ec: ExecutionContext) {
    if (javaFuture.isDone || javaFuture.isCancelled) {
      promise.success(javaFuture.get)
    } else {
      Polling.system.scheduler.scheduleOnce(FiniteDuration(10, TimeUnit.MILLISECONDS)) {
        pollJavaFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      }(ec)
    }
  }

  private def pollCouchbaseFutureUntilDoneOrCancelled[T](javaFuture: java.util.concurrent.Future[T], promise: Promise[OperationStatus], ec: ExecutionContext) {
    if (javaFuture.isDone || javaFuture.isCancelled) {
      javaFuture match {
        case o: OperationFuture[T] => {
          o.get
          promise.success(o.getStatus)
        }
        case h: HttpFuture[T] => {
          h.get
          promise.success(h.getStatus)
        }
      }
    } else {
      Polling.system.scheduler.scheduleOnce(FiniteDuration(10, TimeUnit.MILLISECONDS)) {
        pollCouchbaseFutureUntilDoneOrCancelled(javaFuture, promise, ec)
      }(ec)
    }
  }
}

object Polling {
  val pollingFutures: Boolean = play.api.Play.configuration.getBoolean("couchbase.experimental.pollfutures").getOrElse(false)
  val system = ActorSystem("JavaFutureToScalaFuture")
}
