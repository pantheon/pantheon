package config

import java.util.UUID
import java.util.concurrent.Executors

import controllers._
import management.{CancellableTasksRegistry, QueryDescription}
import play.api.ApplicationLoader.Context
import play.api._
import play.api.db.slick.{DbName, SlickComponents}
import play.filters.HttpFiltersComponents
import services.{
  AllowAllAuthorization,
  AuthorizationRepo,
  BackendConfigRepo,
  CatalogRepo,
  DataSourceProductRepo,
  DataSourceRepo,
  QueryHistoryRepo,
  SavedQueryRepo,
  SchemaRepo,
  Tracing
}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import PantheonAppLoader.{MessageBusProvider, subscriber}
import akka.actor.ActorSystem
import cats.data.EitherT
import com.typesafe.config.Config
import messagebus.MessageBus.{CancelQuery, Subscriber}
import messagebus.MessageBus
import play.api.db.evolutions.EvolutionsComponents
import play.api.db.slick.evolutions.SlickDBApi
import play.api.mvc.Result
import play.api.mvc.Results.NotFound
import Writables.jsonWritable
import jdbc.{Cache, JdbcServer, PantheonMetaImpl}
import play.filters.cors.{CORSConfig, CORSFilter}
import router.Routes
import services.Tracing.JaegerTracerConfig

import scala.concurrent.Future

object PantheonAppLoader {
  type MessageBusProvider = (ActorSystem, Config, Subscriber) => MessageBus

  def subscriber(reg: CancellableTasksRegistry[QueryDescription]): Subscriber = {
    case CancelQuery(catId, id) => Future.fromTry(reg.cancel(id, _.catalogId == catId))
  }
}

class PantheonAppLoader(buildPantheonComponents: Context => PantheonComponents, startJdbcServer: Boolean = true)
    extends ApplicationLoader {
  def load(context: Context): Application = {
    val components = buildPantheonComponents(context)

    LoggerConfigurator(context.environment.classLoader).foreach {
      _.configure(context.environment, context.initialConfiguration, Map.empty)
    }

    println("applying DB evolutions if needed")
    components.applicationEvolutions
    println("finished applying evolutions")

    if (startJdbcServer) {
      JdbcServer.start(components.jdbcMeta)

      context.lifecycle.addStopHook { () =>
        Future.successful(JdbcServer.stop())
      }
    }

    components.application
  }
}

class PantheonComponents(
    val context: Context,
    messageBusProvider: MessageBusProvider,
    dbName: DbName = DbName("default")
) extends BuiltInComponentsFromContext(context)
    with HttpFiltersComponents
    with AssetsComponents
    with SlickComponents
    with EvolutionsComponents { self =>

  // this is required by for 'EvolutionsComponents'

  override val dbApi = SlickDBApi(slickApi)

  val config = configuration.underlying

  val authEnabled = configuration.get[Boolean]("pantheon.auth.enabled")

  val tconf = config.getConfig("pantheon.tracing")
  val traceStarter = if (tconf.getBoolean("enabled")) {
    val config = JaegerTracerConfig(service = tconf.getString("service"),
                                    host = tconf.getString("host"),
                                    port = tconf.getInt("port"),
                                    sync = tconf.getBoolean("sync"))
    println(s"Tracing enabled with: $config")
    Some(Tracing.traceStarter(config))
  } else {
    None
  }

  val hikariThreadPool = {
    val poolParamName = "hikari.houseKeeperThreadPoolSize"
    Executors.newScheduledThreadPool(if (config.hasPath(poolParamName)) config.getInt(poolParamName) else 1)
  }

  val actionBuilder = new PantheonActionBuilder(defaultActionBuilder, authEnabled, traceStarter)

  override lazy val controllerComponents = new PantheonControllerComponents(this)

  override lazy val httpErrorHandler = new ErrorHandler(environment, configuration, sourceMapper, router)

  val dbConfig: DatabaseConfig[JdbcProfile] = slickApi.dbConfig[JdbcProfile](dbName)

  val queryHistoryRepo = new QueryHistoryRepo()(executionContext, dbConfig)

  val authorization =
    if (authEnabled) new AuthorizationRepo()(executionContext, dbConfig)
    else new AllowAllAuthorization

  val dataSourceProductRepo: DataSourceProductRepo = {
    val resourceRoot: String = configuration.get[String]("pantheon.datasources.resource.root")
    new DataSourceProductRepo(resourceRoot)(executionContext, dbConfig)
  }
  val catalogRepo = new CatalogRepo()(executionContext, dbConfig)
  val backendConfigRepo = new BackendConfigRepo()(executionContext, dbConfig)
  val dsRepo = new DataSourceRepo(dataSourceProductRepo, hikariThreadPool)(executionContext, dbConfig)
  // to close all datasources
  // currently disabled because it doesn't completely close connections, busy connections remain
//  context.lifecycle.addStopHook(() => dsRepo.shutdown())

  val schemaRepo = new SchemaRepo(dsRepo)(executionContext, dbConfig)
  val pqRepo = new SavedQueryRepo(schemaRepo)(executionContext, dbConfig)

  val queryRegistry = new CancellableTasksRegistry[QueryDescription]

  val messageBus = messageBusProvider(actorSystem, config, subscriber(queryRegistry))

  val queryHelper = new QueryHelper(queryRegistry, queryHistoryRepo)

  val jdbcMeta = {
    val connectionCacheConfig = Cache.Config.read(config.getConfig("pantheon.jdbc.connectionCache"))
    val statementCacheConfig = Cache.Config.read(config.getConfig("pantheon.jdbc.statementCache"))
    new PantheonMetaImpl(backendConfigRepo, schemaRepo, connectionCacheConfig, statementCacheConfig)(executionContext)
  }

  val connectionProvider = new BackendAwareConnectionProvider(backendConfigRepo, schemaRepo)(executionContext)

  val catalogExistsCheck: UUID => EitherT[Future, Result, Unit] = cid =>
    EitherT(
      catalogRepo.catalogExists(cid).map(Either.cond(_, (), NotFound(ActionError(s"catalog '$cid' not found"))))
  )
  override val router: Routes = new Routes(
    httpErrorHandler,
    new HomeController(controllerComponents, actionBuilder),
    new DataSourceProductController(controllerComponents, actionBuilder, authorization, dataSourceProductRepo, dsRepo),
    new DataSourceProductFileController(controllerComponents, actionBuilder, authorization, dataSourceProductRepo),
    new CatalogController(controllerComponents, actionBuilder, authorization, catalogRepo),
    new BackendConfigController(controllerComponents,
                                actionBuilder,
                                authorization,
                                backendConfigRepo,
                                catalogExistsCheck),
    new SchemaController(controllerComponents, actionBuilder, authorization, schemaRepo, catalogExistsCheck),
    new DataSourceController(controllerComponents, actionBuilder, authorization, dsRepo, catalogExistsCheck),
    new TablesController(controllerComponents, actionBuilder, authorization, schemaRepo, dsRepo),
    new SavedQueryController(controllerComponents, actionBuilder, authorization, pqRepo, catalogExistsCheck),
    new SavedQueryExecController(controllerComponents,
                                 actionBuilder,
                                 queryHelper,
                                 authorization,
                                 pqRepo,
                                 schemaRepo,
                                 connectionProvider)(dbConfig),
    new NativeQueriesController(controllerComponents, actionBuilder, authorization, dsRepo, queryHelper),
    new QueryController(controllerComponents,
                        actionBuilder,
                        queryHelper,
                        authorization,
                        schemaRepo,
                        connectionProvider)(dbConfig),
    new SchemaVerificationController(controllerComponents, actionBuilder, schemaRepo.findAndCompile, authorization),
    new QueryHistoryController(controllerComponents,
                               actionBuilder,
                               authorization,
                               queryHistoryRepo,
                               messageBus.publish(_),
                               catalogExistsCheck),
    new StatusController(controllerComponents, actionBuilder),
    assets
  )

  lazy val corsFilter = new CORSFilter(CORSConfig.fromConfiguration(configuration))

  override lazy val httpFilters = Seq(corsFilter)
}
