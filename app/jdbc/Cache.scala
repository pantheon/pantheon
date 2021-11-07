package jdbc

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification}

import collection.JavaConverters._

object Cache {

  object Config {
    def read(config: com.typesafe.config.Config): Config =
      Config(
        config.getInt("concurrencyLevel"),
        config.getInt("initialCapacity"),
        config.getInt("maximumSize"),
        config.getInt("expiryDuration"),
        TimeUnit.valueOf(config.getString("expiryUnit"))
      )
  }

  case class Config(concurrencyLevel: Int,
                    initialCapacity: Int,
                    maximumSize: Int,
                    expiryDuration: Int,
                    expiryUnit: TimeUnit)

  def create[K <: AnyRef, V <: AnyRef](config: Config,
                                       removalListener: (K, V) => Unit): collection.concurrent.Map[K, V] =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(config.concurrencyLevel)
      .initialCapacity(config.initialCapacity)
      .maximumSize(config.maximumSize)
      .expireAfterAccess(config.expiryDuration, config.expiryUnit)
      .removalListener((notification: RemovalNotification[K, V]) =>
        removalListener(notification.getKey, notification.getValue))
      .build[K, V]()
      .asMap()
      .asScala

}
