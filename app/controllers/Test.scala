package controllers

import collection.JavaConversions._
import collection.mutable.ArrayBuffer

import java.net._
import com.couchbase.client.CouchbaseClient

import play.api.mvc._
import play.api._
import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}

import com.google.gson.Gson

import play.api.Play.current

trait CouchReader[T] {
  def extract(value: Any): Option[T]
}

class Couch(client: Option[CouchbaseClient], host: String, port: String, base: String, bucket: String, pass: String, timeout: Long) {

  def connect() = {
    val uris = ArrayBuffer(URI.create(s"http://$host:$port/$base"))
    val client = new CouchbaseClient(uris, bucket, pass)
    new Couch(Some(client), host, port, base, bucket, pass, timeout)
  }

  def disconnect() = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    new Couch(None, host, port, base, bucket, pass, timeout)
  }

  def withCouch[T](block: CouchbaseClient => T): Option[T] = {
    client.map(block(_))
  }
}

object Couch {

  def currentCouch(implicit app: Application): Couch = app.plugin[CouchPlugin] match {
    case Some(plugin) => plugin.defaultCouch.getOrElse(throw new PlayException("CouchPlugin Error", "Couchbase connection error!"))
    case _            => throw new PlayException("CouchPlugin Error", "The CouchPlugin has not been initialized!")
  }

  implicit val stringReader: CouchReader[String] = new CouchReader[String] {
    override def extract(value: Any): Option[String] = {
      Option(value.asInstanceOf[String])
    }
  }

  implicit val personReader: CouchReader[Person] = new CouchReader[Person] {
    override def extract(value: Any): Option[Person] = {
      Option(new Gson().fromJson(value.asInstanceOf[String], classOf[Person]))
    }
  }

  def apply(
             host: String = Play.configuration.getString("couch.host").getOrElse("127.0.0.1"),
             port: String = Play.configuration.getString("couch.port").getOrElse("8091"),
             base: String = Play.configuration.getString("couch.base").getOrElse("pools"),
             bucket: String = Play.configuration.getString("couch.bucket").getOrElse("default"),
             pass: String = Play.configuration.getString("couch.pass").getOrElse(""),
             timeout: Long = Play.configuration.getLong("couch.timeout").getOrElse(0)): Couch = {
    new Couch(None, host, port, base, bucket, pass, timeout)
  }

  def withCouch[T](block: CouchbaseClient => T): T = currentCouch.withCouch(block).get

  def withSingleCouch[T](block: CouchbaseClient => T): T = {
    val couch = Couch().connect()
    try {
      couch.withCouch(block).get
    } finally {
      couch.disconnect()
    }
  }

  def getAsync[T](key: String)(implicit client: CouchbaseClient, r: CouchReader[T], ec: ExecutionContext): Future[Option[T]] = {
    Future {
      val any: Any = client.get(key) //val any: Any = client.asyncGet(key).get(10, TimeUnit.SECONDS) // memcached support only ???
      any
    }(ec).map(r.extract(_)) // Ok JavaFuture.get is crappy, but don't have the choice ...
  }
}

class CouchPlugin(implicit app: Application) extends Plugin {
  var defaultCouch: Option[Couch] = None
  override def onStart {
    Logger.info("Connection to CouchBase ...")
    defaultCouch = Option(Couch().connect())
    Logger.info("Connected !!!")
  }
  override def onStop {
    Logger.info("Couchbase shutdown")
    defaultCouch = Option(Couch().disconnect())
  }
}

case class Person(name: String, surname: String)

object TestCouchBaseClient extends Controller {

  import controllers.Couch._
  import play.api.libs.concurrent.Execution.Implicits._

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
        getAsync[Person](key).map { opt =>
          opt.map(person => Ok(person.toString)).getOrElse(BadRequest(s"Unable to find person with key: $key"))
        }
      }
    }
  }
}
