package couchbase

import play.api.libs.json._

object CouchbaseWrites {
  implicit val stringToDocumentWriter = new Writes[String] {
    def writes(o: String): JsValue = {
      Json.parse(o)
    }
  }
}
