package config

import messagebus.RedisMessageBus

class ClusterAppLoaderRedis
    extends PantheonAppLoader(
      new PantheonComponents(
        _,
        messageBusProvider = (actorSystem, conf, sunscriber) =>
          new RedisMessageBus(
            conf.getString("redis.host"),
            conf.getInt("redis.port"),
            conf.getString("redis.message.channel"),
            sunscriber,
            actorSystem.dispatcher
        )
      )
    )
