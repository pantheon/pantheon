package pantheon.backend.spark

import org.apache.spark.sql.SparkSession
import pantheon.backend.{Backend, BackendConnection}
import pantheon.schema.Schema

object SparkBackend {
  def apply(params: Map[String, String] = Map.empty): SparkBackend = {
    val b = SparkSession.builder

    // these are sensible default values that can be overridden by params
    b.appName("Pantheon").master("local")

    for ((key, value) <- params) {
      b.config(key, value)
    }

    new SparkBackend(b.getOrCreate())
  }
}

class SparkBackend(sparkSession: SparkSession) extends Backend {
  override def getConnection(schema: Schema): BackendConnection =
    new SparkConnection(schema, sparkSession)
}
