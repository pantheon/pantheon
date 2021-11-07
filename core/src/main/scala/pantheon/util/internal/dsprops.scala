package pantheon.util.internal
import cats.data.{EitherT, State}
import pantheon.DataSource.{DriverClassLoc, MySqlProtocol}
import cats.syntax.either._
import cats.syntax.traverse._
import cats.instances.option._
import cats.instances.either._
import pantheon.JdbcDataSource.{DriverClassName, DriverClassPath}
import EitherT._
import cats.Functor

object dsprops {

  type User = String
  type Password = Option[String]
  type Props = Map[String, String]
  type PropsState[T] = State[Props, T]
  val (hostKey, portKey, dataBaseKey, schemaKey, protocolKey, urlKey, userKey, passwordKey) =
    ("host", "port", "database", "schema", "protocol", "url", "user", "password")

  def mustBeDefinedMsg(lbl: String) = s"'$lbl' must be defined"

  // reads optional pantheon-specific param, removes key from props after reading
  def optional(key: String): PropsState[Option[String]] =
    State(props => (props - key) -> props.get(key))

  // reads mandatory pantheon-specific param, removes key from props after reading
  def mandatory(key: String): EitherT[PropsState, String, String] =
    EitherT(optional(key).map(_.toRight(mustBeDefinedMsg(key))))

  private def parsePort(port: String): Either[String, Int] =
    Either.catchNonFatal(port.toInt).leftMap(e => "malformed port: " + e.getMessage)

  val readPort: EitherT[PropsState, String, Int] = mandatory(portKey).subflatMap(parsePort)
  val readPortOpt: EitherT[PropsState, String, Option[Int]] = EitherT(optional(portKey).map(_.traverse(parsePort)))

  def parseMysqlProtocol(proto: String): Either[String, MySqlProtocol.Value] =
    Either.catchNonFatal(MySqlProtocol.withName(proto)).leftMap(_ => s"unknown mysql protocol: $proto")

  val readDriverClassLoc: EitherT[PropsState, String, Option[DriverClassLoc]] =
    for {
      name <- liftF[PropsState, String, Option[String]](optional(DriverClassName))
      path <- liftF(optional(DriverClassPath))
      classLoc <- cond[PropsState](
        path.isEmpty || name.isDefined,
        name.map(DriverClassLoc(_, path)),
        "class name must be defined when class path is defined"
      )
    } yield classLoc
}
