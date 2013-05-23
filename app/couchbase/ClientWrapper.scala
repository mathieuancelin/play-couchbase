package couchbase
import play.api.libs.json._
import com.couchbase.client.CouchbaseClient
import scala.concurrent.{Future, ExecutionContext}
import net.spy.memcached.ops.OperationStatus
import com.couchbase.client.protocol.views.{SpatialView, DesignDocument, Query, View}
import collection.JavaConversions._
import net.spy.memcached.{ReplicateTo, PersistTo}

// Yeah I know JavaFuture.get is really ugly, but what can I do ???
// http://stackoverflow.com/questions/11529145/how-do-i-wrap-a-java-util-concurrent-future-in-an-akka-future
trait ClientWrapper {

  def find[T](view: View, query: Query)(implicit client: CouchbaseClient, r: Reads[T], ec: ExecutionContext): Future[List[T]] = {
    println(view.getURI)
    println(query.toString)
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

  def view(docName: String, viewName: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[View] = {
    Future {
      client.asyncGetView(docName, viewName).get()
    }(ec)
  }

  def spatialView(docName: String, viewName: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[SpatialView] = {
    Future {
      client.asyncGetSpatialView(docName, viewName).get()
    }(ec)
  }

  def designDocument(docName: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[DesignDocument[_]] = {
    Future {
      client.asyncGetDesignDocument(docName).get()
    }(ec)
  }

  def createDesignDoc(name: String, value: JsObject)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      client.asyncCreateDesignDoc(name, Json.stringify(value)).getStatus
    }(ec)
  }

  def createDesignDoc(name: String, value: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      client.asyncCreateDesignDoc(name, value).getStatus
    }(ec)
  }

  def createDesignDoc(value: DesignDocument[_])(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      client.asyncCreateDesignDoc(value).getStatus
    }(ec)
  }

  def deleteDesignDoc(name: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      client.asyncDeleteDesignDoc(name).getStatus
    }(ec)
  }

  def keyStats(key: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[Map[String, String]] = {
    Future {
       client.getKeyStats(key).get().toMap
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

  def set[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.set(key, exp, value, replicateTo)
        future.get
      future.getStatus
    }(ec)
  }
  def set[T](key: String, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, 0, value, replicateTo)(client, w, ec)
  }
  def set[T](key: String, exp: Int, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.set(key, exp, value, peristTo)
        future.get
      future.getStatus
    }(ec)
  }
  def set[T](key: String, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, 0, value, peristTo)(client, w, ec)
  }
  def set[T](key: String, exp: Int, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.set(key, exp, value, peristTo, replicateTo)
        future.get
      future.getStatus
    }(ec)
  }
  def set[T](key: String, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    set(key, 0, value, peristTo, replicateTo)(client, w, ec)
  }

  def add[T](key: String, exp: Int, value: T)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    //ClientWrapperPoller.promise(client.add(key, exp, Json.stringify(w.writes(value))))
    Future {
      val future = client.add(key, exp, Json.stringify(w.writes(value)))
      future.get
      future.getStatus
    }(ec)
  }

  def add[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.add(key, exp, value, replicateTo)
        future.get
      future.getStatus
    }(ec)
  }
  def add[T](key: String, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, 0, value, replicateTo)(client, w, ec)
  }
  def add[T](key: String, exp: Int, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.add(key, exp, value, peristTo)
        future.get
      future.getStatus
    }(ec)
  }
  def add[T](key: String, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, 0, value, peristTo)(client, w, ec)
  }
  def add[T](key: String, exp: Int, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.add(key, exp, value, peristTo, replicateTo)
        future.get
      future.getStatus
    }(ec)
  }
  def add[T](key: String, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    add(key, 0, value, peristTo, replicateTo)(client, w, ec)
  }

  def delete(key: String)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    //ClientWrapperPoller.promise(client.delete(key))
    Future {
      val future = client.delete(key)
      future.get
      future.getStatus
    }(ec)
  }

  def delete[T](key: String, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.delete(key, replicateTo)
        future.get
      future.getStatus
    }(ec)
  }
  def delete[T](key: String, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.delete(key, peristTo)
        future.get
      future.getStatus
    }(ec)
  }
  def delete[T](key: String, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.delete(key, peristTo, replicateTo)
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

  def replace[T](key: String, exp: Int, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.replace(key, exp, value, replicateTo)
        future.get
      future.getStatus
    }(ec)
  }
  def replace[T](key: String, value: T, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
     replace(key, 0, value, replicateTo)(client, w, ec)
  }
  def replace[T](key: String, exp: Int, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.replace(key, exp, value, peristTo)
        future.get
      future.getStatus
    }(ec)
  }
  def replace[T](key: String, value: T, peristTo: PersistTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, 0, value, peristTo)(client, w, ec)
  }
  def replace[T](key: String, exp: Int, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.replace(key, exp, value, peristTo, replicateTo)
      future.get
      future.getStatus
    }(ec)
  }
  def replace[T](key: String, value: T, peristTo: PersistTo, replicateTo: ReplicateTo)(implicit client: CouchbaseClient, w: Writes[T], ec: ExecutionContext): Future[OperationStatus] = {
    replace(key, 0, value, peristTo,replicateTo)(client, w, ec)
  }

  def flush(delay: Int)(implicit client: CouchbaseClient, ec: ExecutionContext): Future[OperationStatus] = {
    Future {
      val future = client.flush(delay)
      future.get
      future.getStatus
    }(ec)
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
}

/*
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
*/
