package pantheon

import java.net.URI
import java.sql.{Date, ResultSet}

import org.apache.calcite.adapter.druid.DruidQuery
import org.apache.calcite.rel.RelNode
import org.scalatest.{MustMatchers, WordSpec}
import pantheon.util.{Logging, RowSetUtil}
import pantheon.planner.ASTBuilders._
import cats.syntax.either._
import pantheon.backend.calcite.CalcitePlan
import pantheon.schema.{Schema, SchemaCompiler}
import pantheon.schema.parser.SchemaParser
import pantheon.util.Tap

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.Try

class E360Spec extends WordSpec with MustMatchers {

  implicit val ctx = Logging.newContext

  class Fixture {
    val ds =
      DruidSource("360_events", new URI("http://localhost:3000"), new URI("http://localhost:3001"), false, false, false)

    val schemaPsl =
      """schema Events360 (dataSource = "360_events") {
        |  dimension Date (table = "Events") {
        |    attribute year(castType = "date")
        |    attribute month(castType = "date")
        |    attribute day(castType = "date")
        |    attribute time (column = "__time")
        |  }
        |
        |  dimension Event (table = "Events") {
        |    attribute id (column = "_event_id")
        |    attribute receivedAt (column = "_received_at")
        |    attribute customer (column = "customer")
        |    attribute event (column = "event")
        |    attribute name (column = "name")
        |    attribute src (column = "src")
        |  }
        |
        |  dimension Execution (table = "Events") {
        |    attribute customer (column = "execution:context:customer")
        |    attribute dataGatewaySoftDelete (column = "execution:context:dataGateway:softDelete:enabled")
        |    attribute campaignExecutionId (column = "execution:ispb:messageContextForSender:data:campaignExecutionId")
        |    attribute campaignId (column = "execution:ispb:messageContextForSender:data:campaignId")
        |    attribute persistedAt (column = "execution:ispb:messageContextForSender:data:internal:ispb:persistedAt")
        |    attribute notificationId (column = "execution:ispb:messageContextForSender:data:notificationId")
        |    attribute dataTime (column = "execution:ispb:messageContextForSender:data:time")
        |    attribute recovered (column = "execution:ispb:messageContextForSender:recovered")
        |    attribute timeToEvent (column = "execution:profiler:time:toEvent:microseconds")
        |    attribute timeToFlush (column = "execution:profiler:time:toFlush:microseconds")
        |    attribute proxyExternalHost (column = "execution:proxy:external:host:name")
        |  }
        |
        |  dimension Payload (table = "Events") {
        |    attribute appId (column = "payload:appId")
        |    attribute appIdentifier (column = "payload:appIdentifier")
        |    attribute appName (column = "payload:appName")
        |    attribute appStartId (column = "payload:appStartId")
        |    attribute bannerClicked (column = "payload:bannerClicked")
        |    attribute bannerClosed (column = "payload:bannerClosed")
        |    attribute bannerFailedToLoad (column = "payload:bannerFailedToLoad")
        |    attribute bannerId (column = "payload:bannerId")
        |    attribute bannerLeftApplication (column = "payload:bannerLeftApplication")
        |    attribute bannerLoaded (column = "payload:bannerLoaded")
        |    attribute bannerOpened (column = "payload:bannerOpened")
        |    attribute bigIconNotUsedReason (column = "payload:bigIconNotUsedReason")
        |    attribute bigIconRequested (column = "payload:bigIconRequested")
        |    attribute bigIconUsed (column = "payload:bigIconUsed")
        |    attribute bigPictureNotUsedReason (column = "payload:bigPictureNotUsedReason")
        |    attribute bigPictureRequested (column = "payload:bigPictureRequested")
        |    attribute bigPictureUsed (column = "payload:bigPictureUsed")
        |    attribute campaignId (column = "payload:campaignId")
        |    attribute categoryName (column = "payload:categoryName")
        |    attribute deeplink (column = "payload:deeplink")
        |    attribute eventName (column = "payload:eventName")
        |    attribute indirectOpenDetected (column = "payload:indirectOpenDetected")
        |    attribute indirectOpenElapsedTime (column = "payload:indirectOpenElapsedTime")
        |    attribute indirectOpenTimeout (column = "payload:indirectOpenTimeout")
        |    attribute installCase (column = "payload:installCase")
        |    attribute name (column = "payload:name")
        |    attribute notificationId (column = "payload:notificationId")
        |    attribute placement (column = "payload:placement")
        |    attribute previousState (column = "payload:previousState")
        |    attribute provider (column = "payload:provider")
        |    attribute reason (column = "payload:reason")
        |    attribute senderId (column = "payload:senderId")
        |    attribute tutorialStep (column = "payload:tutorialStep")
        |    attribute vibratePossible (column = "payload:vibratePossible")
        |    attribute vibrateRequested (column = "payload:vibrateRequested")
        |  }
        |
        |  dimension Request (table = "Events") {
        |    attribute connectionInfo (column = "request:connectionInfo")
        |    attribute contentType (column = "request:content:type")
        |    attribute id (column = "request:id")
        |    attribute method (column = "request:method")
        |    attribute methodReal (column = "request:method:real")
        |    attribute route (column = "request:route")
        |    attribute serverHttpHost (column = "request:server:httpHost")
        |    attribute time (column = "request:time")
        |    attribute timeEpoch (column = "request:time:epoch")
        |    attribute uri (column = "request:uri")
        |    attribute userAgent (column = "request:user:agent")
        |  }
        |
        |  dimension SpecCrm (table = "Events") {
        |    attribute asrBannerId (column = "spec:crm:asr:banner:id")
        |    attribute asrNotificationId (column = "spec:crm:asr:notification:id")
        |    attribute asrNotificationLanguage (column = "spec:crm:asr:notification:language")
        |    attribute clientEventNo (column = "spec:crm:client:event:no")
        |    attribute clientTime (column = "spec:crm:client:time:local:timestamp")
        |    attribute cpmCampaignExecutionId (column = "spec:crm:cmp:campaign:execution:id")
        |    attribute cpmCampaignId (column = "spec:crm:cmp:campaign:id")
        |    attribute cmpCampaignStepId (column = "spec:crm:cmp:campaign:step:id")
        |    attribute crmCustomerAppId (column = "spec:crm:crm:customerApp:id")
        |    attribute crmCustomerAppOsName (column = "spec:crm:crm:customerApp:os:name")
        |    attribute crmPersonCreatedAt (column = "spec:crm:crm:person:createdAt")
        |    attribute crmPersonId (column = "spec:crm:crm:person:id")
        |    attribute elAppInstanceCustomerMigratorQueued (column = "spec:crm:el:appInstance:customerMigrator:queued")
        |    attribute elAppInstanceUnknown (column = "spec:crm:el:appInstance:unknown")
        |    attribute senPackageId (column = "spec:crm:sen:package:id")
        |    attribute senProcess (column = "spec:crm:sen:process")
        |    attribute senPushError (column = "spec:crm:sen:push:error")
        |    attribute senQueueName (column = "spec:crm:sen:queue:name")
        |    attribute senSenderId (column = "spec:crm:sen:sender:id")
        |  }
        |
        |  dimension SpecCrmAppInstance (table = "Events") {
        |    attribute edsBadgeCount (column = "spec:crm:crm:appInstace:eds:badgeCount")
        |    attribute appVersion (column = "spec:crm:crm:appInstance:app:version")
        |    attribute controlGroup (column = "spec:crm:crm:appInstance:control-group")
        |    attribute createdAt (column = "spec:crm:crm:appInstance:createdAt")
        |    attribute installCase (column = "spec:crm:crm:appInstance:install:case")
        |    attribute lastActiveTime (column = "spec:crm:crm:appInstance:lastActive:time")
        |    attribute notificationOptIn (column = "spec:crm:crm:appInstance:notification:optIn")
        |    attribute pushEnabled (column = "spec:crm:crm:appInstance:push:enabled")
        |    attribute pushOptIn (column = "spec:crm:crm:appInstance:push:optIn")
        |    attribute sdkVersion (column = "spec:crm:crm:appInstance:sdk:version")
        |    attribute shortId (column = "spec:crm:crm:appInstance:short:id")
        |    attribute shortId1 (column = "spec:crm:crm:appInstance:shortId")
        |    attribute state (column = "spec:crm:crm:appInstance:state")
        |    attribute testId (column = "spec:crm:crm:appInstance:test:id")
        |    attribute trackingOptIn (column = "spec:crm:crm:appInstance:tracking:optIn")
        |  }
        |
        |  dimension SpecCrmDevice (table = "Events") {
        |    attribute createdAt (column = "spec:crm:crm:device:createdAt")
        |    attribute deviceType (column = "spec:crm:crm:device:device:type")
        |    attribute id (column = "spec:crm:crm:device:id")
        |    attribute language (column = "spec:crm:crm:device:language")
        |    attribute locale (column = "spec:crm:crm:device:locale")
        |    attribute localeNormalized (column = "spec:crm:crm:device:locale:normalized")
        |    attribute localeNormalizedLanguage (column = "spec:crm:crm:device:locale:normalized:language")
        |    attribute localeNormalizedLanguageVariant (column = "spec:crm:crm:device:locale:normalized:languageVariant")
        |    attribute osName (column = "spec:crm:crm:device:os:name")
        |    attribute osVersion (column = "spec:crm:crm:device:os:version")
        |    attribute osVersionNormalized (column = "spec:crm:crm:device:os:version:normalized")
        |    attribute osVersionNormalizedMajor (column = "spec:crm:crm:device:os:version:normalized:major")
        |    attribute osVersionNormalizedMajorMinor (column = "spec:crm:crm:device:os:version:normalized:majorMinor")
        |    attribute simExists (column = "spec:crm:crm:device:sim:exists")
        |    attribute deviceVendor (column = "spec:crm:crm:device:vendor")
        |  }
        |
        |  measure count(aggregate = "sum", column = "Events.count")
        |  measure uniqueUsers(aggregate = "distinctCount", column = "Events.unique_users")
        |
        |  table Events(physicalTable = "360_events") {
        |    column year (expression = "FLOOR(__time to YEAR)", dimensionRef = "Date.year")
        |    column month (expression = "FLOOR(__time to MONTH)", dimensionRef = "Date.month")
        |    column day (expression = "FLOOR(__time to DAY)", dimensionRef = "Date.day")
        |
        |    column unique_users
        |    column count
        |  }
        |}
        |""".stripMargin

    val catalog = new InMemoryCatalog(Map("Events360" -> schemaPsl), List(ds))
  }

  class Result(statement: Statement) {
    def plan: String = statement.backendLogicalPlan.toString

    def physicalPlan: String = statement.backendPhysicalPlan.toString

    def druidQuery: String = {
      Try(statement.backendPhysicalPlan.asInstanceOf[CalcitePlan]).toOption
        .flatMap(p => findDruidQuery(p.rel))
        .getOrElse("Druid query was not found")
    }

    def findDruidQuery(rel: RelNode): Option[String] = {
      rel match {
        case q: DruidQuery =>
          Some(q.getQueryString)
        case _ =>
          if (rel.getInputs.isEmpty) None
          else rel.getInputs.asScala.map(findDruidQuery).find(_.nonEmpty).flatten
      }
    }

    def value(limit: Int = 100): String =
      RowSetUtil.toString(statement.execute(), limit)
  }

  val fixture = new Fixture

  "Pantheon" when {

    def test(q: PantheonQuery)(block: Result => Unit): Unit = {
      Pantheon.withConnection(fixture.catalog, "Events360") { connection =>
        val statement = connection.createStatement(q).tap(_.mustBe('right))
        block(new Result(statement))
      }
    }

    "querying 360 data" should {
      "get timeseries for month" ignore {
        val query = AggregateQuery(rows = List("Date.month"))

        test(query) { result =>
          result.plan must include
          """LogicalProject(Date.month=[CAST($0):DATE NOT NULL])
              |  LogicalAggregate(group=[{0}])
              |    LogicalProject(Date.month=[$149])
              |      LogicalProject(__time=[$0], _event_id=[$1], """.stripMargin

          result.plan must include
          """DruidQuery(table=[[360_events, 360_events]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]])""".stripMargin

          result.physicalPlan mustBe
            """EnumerableInterpreter
              |  BindableProject(Date.month=[CAST($0):DATE NOT NULL])
              |    DruidQuery(table=[[360_events, 360_events]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]], projects=[[FLOOR($0, FLAG(MONTH))]], groups=[{0}], aggs=[[]])
              |""".stripMargin

          result.value(5) mustBe
            """Date.month=2017-09-01""".stripMargin
        }
      }

      "get timeseries for day" ignore {
        val query = AggregateQuery(rows = List("Date.day"), measures = List("Measures.count"))

        test(query) { result =>
          result.plan must include
          """LogicalProject(Date.day=[CAST($0):DATE NOT NULL], Measures.count=[$1])
            |  LogicalAggregate(group=[{0}], Measures.count=[SUM($1)])
            |    LogicalProject(Date.day=[$149], Measures.count=[$4])
            |      LogicalProject(__time=[$0], _event_id=[$1], _received_at=[$2], activity_kind=[$3]""".stripMargin

          result.plan must include
          """DruidQuery(table=[[360_events, 360_events]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]])""".stripMargin

          result.physicalPlan mustBe
            """EnumerableInterpreter
              |  BindableProject(Date.day=[CAST($0):DATE NOT NULL], Measures.count=[$1])
              |    DruidQuery(table=[[360_events, 360_events]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]], projects=[[$4, FLOOR($0, FLAG(DAY))]], groups=[{1}], aggs=[[SUM($0)]])
              |""".stripMargin

          result.value(5) mustBe
            """Date.day=2017-09-18; Measures.count=37653
              |Date.day=2017-09-19; Measures.count=962347""".stripMargin
        }
      }

      "push filters" ignore {
        val query = AggregateQuery(
          rows = List("Event.event", "Date.month"),
          measures = List("Measures.uniqueUsers", "Measures.count"),
          filter = Some(
            ref("Date.time") >= lit(Date.valueOf("2017-08-01")) &
              ref("Date.time") < lit(Date.valueOf("2017-10-01"))
          )
        )

        test(query) { result =>
          result.plan mustBe
            """LogicalProject(Event.event=[$0], Date.month=[CAST($1):DATE NOT NULL], Measures.uniqueUsers=[$2], Measures.count=[$3])
              |  LogicalAggregate(group=[{0, 1}], Measures.uniqueUsers=[COUNT(DISTINCT $2)], Measures.count=[SUM($3)])
              |    LogicalProject(Event.event=[$6], Date.month=[$149], Measures.uniqueUsers=[$150], Measures.count=[$4])
              |      LogicalFilter(condition=[AND(>=($0, 2017-08-01), <($0, 2017-10-01))])
              |        LogicalProject(__time=[$0], _event_id=[$1], _received_at=[$2], activity_kind=[$3], count=[$4], customer=[$5], event=[$6], execution:context:customer=[$7], execution:context:dataGateway:softDelete:enabled=[$8], execution:ispb:lastOverlayForAppInstance:recovered=[$9], execution:ispb:messageContextForSender:data:campaignExecutionId=[$10], execution:ispb:messageContextForSender:data:campaignId=[$11], execution:ispb:messageContextForSender:data:internal:ispb:persistedAt=[$12], execution:ispb:messageContextForSender:data:notificationId=[$13], execution:ispb:messageContextForSender:data:time=[$14], execution:ispb:messageContextForSender:recovered=[$15], execution:profiler:time:toEvent:microseconds=[$16], execution:profiler:time:toFlush:microseconds=[$17], execution:proxy:external:host:name=[$18], name=[$19], payload:announcerNotificationId=[$20], payload:appId=[$21], payload:appIdentifier=[$22], payload:appName=[$23], payload:appStartId=[$24], payload:bannerClicked=[$25], payload:bannerClosed=[$26], payload:bannerDownloaded=[$27], payload:bannerFailedToLoad=[$28], payload:bannerId=[$29], payload:bannerLeftApplication=[$30], payload:bannerLoaded=[$31], payload:bannerNotShown=[$32], payload:bannerOpened=[$33], payload:bigIconNotUsedReason=[$34], payload:bigIconRequested=[$35], payload:bigIconUsed=[$36], payload:bigPictureNotUsedReason=[$37], payload:bigPictureRequested=[$38], payload:bigPictureUsed=[$39], payload:campaignId=[$40], payload:campaignStepId=[$41], payload:categoryName=[$42], payload:controlGroupCampaignsDisabled=[$43], payload:deeplink=[$44], payload:eventName=[$45], payload:firstStartDialogsSuppressed=[$46], payload:indirectOpenDetected=[$47], payload:indirectOpenElapsedTime=[$48], payload:indirectOpenTimeout=[$49], payload:installCase=[$50], payload:installReferrer=[$51], payload:name=[$52], payload:notificationId=[$53], payload:overlayId=[$54], payload:placement=[$55], payload:previousState=[$56], payload:provider=[$57], payload:reason=[$58], payload:senderId=[$59], payload:tutorialStep=[$60], payload:vibratePossible=[$61], payload:vibrateRequested=[$62], request:connectionInfo=[$63], request:content:type=[$64], request:id=[$65], request:method=[$66], request:method:real=[$67], request:route=[$68], request:server:httpHost=[$69], request:time=[$70], request:time:epoch=[$71], request:uri=[$72], request:user:agent=[$73], spec:crm:asr:banner:id=[$74], spec:crm:asr:notification:id=[$75], spec:crm:asr:overlay:id=[$76], spec:crm:client:event:no=[$77], spec:crm:client:googlePlayServices:available=[$78], spec:crm:client:time:local:timestamp=[$79], spec:crm:cmp:campaign:execution:id=[$80], spec:crm:cmp:campaign:id=[$81], spec:crm:cmp:campaign:step:id=[$82], spec:crm:crm:appInstace:eds:badgeCount=[$83], spec:crm:crm:appInstance:app:version=[$84], spec:crm:crm:appInstance:control-group=[$85], spec:crm:crm:appInstance:createdAt=[$86], spec:crm:crm:appInstance:install:case=[$87], spec:crm:crm:appInstance:lastActive:time=[$88], spec:crm:crm:appInstance:notification:optIn=[$89], spec:crm:crm:appInstance:push:enabled=[$90], spec:crm:crm:appInstance:push:optIn=[$91], spec:crm:crm:appInstance:sdk:version=[$92], spec:crm:crm:appInstance:short:id=[$93], spec:crm:crm:appInstance:shortId=[$94], spec:crm:crm:appInstance:state=[$95], spec:crm:crm:appInstance:test:id=[$96], spec:crm:crm:appInstance:tracking:optIn=[$97], spec:crm:crm:customerApp:id=[$98], spec:crm:crm:device:createdAt=[$99], spec:crm:crm:device:device:type=[$100], spec:crm:crm:device:id=[$101], spec:crm:crm:device:language=[$102], spec:crm:crm:device:locale=[$103], spec:crm:crm:device:locale:normalized=[$104], spec:crm:crm:device:locale:normalized:language=[$105], spec:crm:crm:device:locale:normalized:languageVariant=[$106], spec:crm:crm:device:os:name=[$107], spec:crm:crm:device:os:version=[$108], spec:crm:crm:device:os:version:normalized=[$109], spec:crm:crm:device:os:version:normalized:major=[$110], spec:crm:crm:device:os:version:normalized:majorMinor=[$111], spec:crm:crm:device:sim:exists=[$112], spec:crm:crm:device:vendor=[$113], spec:crm:crm:person:createdAt=[$114], spec:crm:crm:person:id=[$115], spec:crm:el:appInstance:customerMigrator:queued=[$116], spec:crm:el:appInstance:unknown=[$117], spec:crm:sen:sender:id=[$118], spec:miyuki:ad:channel=[$119], spec:miyuki:ad:placement=[$120], spec:miyuki:channel:user:id=[$121], spec:miyuki:device:country=[$122], spec:miyuki:device:hash=[$123], spec:miyuki:device:locale=[$124], spec:miyuki:device:os=[$125], spec:miyuki:device:type=[$126], spec:miyuki:ident=[$127], spec:miyuki:ident:row=[$128], spec:miyuki:offer:application:store_id=[$129], spec:miyuki:offer:campaign:hash=[$130], spec:miyuki:offer:campaign:name=[$131], spec:miyuki:offer:creative:tag=[$132], spec:miyuki:offer:id=[$133], spec:miyuki:offer:is_incentive=[$134], spec:miyuki:offer:name=[$135], spec:miyuki:offer:network:account:id=[$136], spec:miyuki:offer:network:id=[$137], spec:miyuki:offer:network:name=[$138], spec:miyuki:payout:eur=[$139], spec:miyuki:session:id=[$140], spec:miyuki:stats:3m:monetizations=[$141], spec:miyuki:stats:7d:monetizations=[$142], spec:miyuki:stats:timestamp=[$143], spec:miyuki:time:click=[$144], spec:miyuki:time:monetization=[$145], spec:miyuki:user:id=[$146], spec:miyuki:v2_app_instance_id=[$147], src=[$148], month=[FLOOR($0, FLAG(MONTH))], unique_users=[$149])
              |          DruidQuery(table=[[360_events, 360_events]], intervals=[[1900-01-01T00:00:00.000/3000-01-01T00:00:00.000]])
              |""".stripMargin

          result.physicalPlan mustBe
            """EnumerableInterpreter
              |  BindableProject(Event.event=[$0], Date.month=[CAST($1):DATE NOT NULL], Measures.uniqueUsers=[$2], Measures.count=[$3])
              |    DruidQuery(table=[[360_events, 360_events]], intervals=[[2017-08-01T00:00:00.000/2017-10-01T00:00:00.000]], projects=[[$6, FLOOR($0, FLAG(MONTH)), $149, $4]], groups=[{0, 1}], aggs=[[COUNT(DISTINCT $2), SUM($3)]])
              |""".stripMargin

          result.value(5) mustBe
            """Event.event=app_AppOpened; Date.month=2017-09-01; Measures.uniqueUsers=254647; Measures.count=493338
              |Event.event=app_AppOpenedFirstTime; Date.month=2017-09-01; Measures.uniqueUsers=2859; Measures.count=2797
              |Event.event=bnr_BannerClicked; Date.month=2017-09-01; Measures.uniqueUsers=207; Measures.count=231
              |Event.event=bnr_BannerClosed; Date.month=2017-09-01; Measures.uniqueUsers=8357; Measures.count=25397
              |Event.event=bnr_BannerOpened; Date.month=2017-09-01; Measures.uniqueUsers=13262; Measures.count=40184""".stripMargin
        }
      }

    }
  }

}
