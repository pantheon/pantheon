package config

import messagebus.SingleNodeMessageBus

class SingleNodeAppLoader
    extends PantheonAppLoader(
      new PantheonComponents(_, (as, _, mp) => new SingleNodeMessageBus(mp, as.dispatcher))
    )
