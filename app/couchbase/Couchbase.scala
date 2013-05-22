package couchbase

import com.couchbase.client.CouchbaseClient
import java.net.URI
import java.util.concurrent.TimeUnit
import play.api.{Play, PlayException, Application}
import collection.JavaConversions._
import collection.mutable.ArrayBuffer
import play.api.Play.current
import scala.Some

class Couchbase(val client: Option[CouchbaseClient], val host: String, val port: String, val base: String, val bucket: String, val pass: String, val timeout: Long) {

  def connect() = {
    val uris = ArrayBuffer(URI.create(s"http://$host:$port/$base"))
    val client = new CouchbaseClient(uris, bucket, pass)
    new Couchbase(Some(client), host, port, base, bucket, pass, timeout)
  }

  def disconnect() = {
    client.map(_.shutdown(timeout, TimeUnit.SECONDS))
    new Couchbase(None, host, port, base, bucket, pass, timeout)
  }

  def withCouchbase[T](block: CouchbaseClient => T): Option[T] = {
    client.map(block(_))
  }
}

object Couchbase extends ClientWrapper {

  private val initMessage = "The CouchbasePlugin has not been initialized! Please edit your conf/play.plugins file and add the following line: '400:couchbase.CouchbasePlugin' (400 is an arbitrary priority and may be changed to match your needs)."
  private val connectMessage = "The CouchbasePlugin doesn't seems to be connected to a Couchbase server. Maybe an error occured!"

  def currentCouch(implicit app: Application): Couchbase = app.plugin[CouchbasePlugin] match {
    case Some(plugin) => plugin.defaultCouch.getOrElse(throw new PlayException("CouchbasePlugin Error", connectMessage))
    case _ => throw new PlayException("CouchbasePlugin Error", initMessage)
  }

  def apply(
             host: String = Play.configuration.getString("couch.host").getOrElse("127.0.0.1"),
             port: String = Play.configuration.getString("couch.port").getOrElse("8091"),
             base: String = Play.configuration.getString("couch.base").getOrElse("pools"),
             bucket: String = Play.configuration.getString("couch.bucket").getOrElse("default"),
             pass: String = Play.configuration.getString("couch.pass").getOrElse(""),
             timeout: Long = Play.configuration.getLong("couch.timeout").getOrElse(0)): Couchbase = {
    new Couchbase(None, host, port, base, bucket, pass, timeout)
  }

  def withCouchbase[T](block: CouchbaseClient => T): T = currentCouch.withCouchbase(block).get

  def withSingleCouchbase[T](block: CouchbaseClient => T): T = {
    val couch = Couchbase().connect()
    try {
      couch.withCouchbase(block).get
    } finally {
      couch.disconnect()
    }
  }
}
