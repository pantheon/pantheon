package controllers.helpers

import java.util.UUID

import cats.Semigroupal
import cats.data.EitherT
import config.Authentication.User
import controllers.AuthorizationControllerCommons._
import controllers.helpers.CrudlActions._
import controllers.{handlePantheonErr, idNotFound}
import errors.{PantheonError, RepoError}
import play.api.mvc.Result
import play.api.mvc.Results.{Created, Ok}
import play.api.libs.json.{Json, Writes}
import services.Authorization.{ActionType, PermissionTO}
import services.{Authorization, |}
import controllers.Writables._
import cats.instances.option._
import pantheon.util.ConcreteChild
import cats.Show
import cats.instances.future._
import services.CatalogRepo.CatalogId

import scala.language.higherKinds
import scala.concurrent.{ExecutionContext, Future}

object CrudlActions {
  case class Page(current: Int, itemsPerPage: Int, itemCount: Int)
  // this class is inherited in controllers to facilitate openapi integration which does not support type parameters
  trait PagedResponse[T] {
    def page: Page
    def data: Seq[T]
  }

  implicit val pageWrites: Writes[Page] = Json.writes[Page]
  implicit def pagedResponseWrites[R: Writes]: Writes[PagedResponse[R]] =
    r => Json.obj("page" -> r.page, "data" -> r.data)

  // may be reused in unauthenticated CRUDL controllers (or inline otherwise)
  def foldCreated[T: Writes](created: PantheonError | T): Result = created.fold(handlePantheonErr(_), Created(_))
  def foldDeleted[K: Show](key: K, deleted: RepoError | Boolean): Result =
    deleted.fold(handlePantheonErr(_), v => if (v) Ok else idNotFound(key))
  def foldOptional[K: Show, T: Writes](key: K, v: Option[T]): Result = v.fold(idNotFound(key))(Ok(_))
  def foldOptional[K: Show, T: Writes](key: K, v: Either[RepoError, Option[T]]): Result =
    v.fold(handlePantheonErr, _.fold(idNotFound(key))(Ok(_)))

  def paginate[T](s: Seq[T], page: Int, pageSize: Int): (Page, Seq[T]) = {
    val startInd = pageSize * (page - 1)
    val paged = s.slice(startInd, startInd + pageSize)
    (Page(page, pageSize, s.size), paged)
  }

  type Key = (ResourceId, PermissionById)
  type PermissionById = ResourceId
  type CatId = ResourceId
}

class CrudlActions[Res, Opt[x] <: Option[x]](auth: Authorization,
                                             resourceType: Authorization.ResourceType,
                                             extractId: Res => ResourceId,
                                             chkCatalogExists: Opt[UUID => EitherT[Future, Result, Unit]])(
    implicit wr: Writes[Res],
    ec: ExecutionContext,
    c: ConcreteChild[Opt[_], Option[_]]) {
  implicit val resourceIdShow: Show[(ResourceId, Opt[CatalogId])] = {
    case (resId, catIdOpt) => s"$resourceType '$resId'" + catIdOpt.fold("")(id => s" from catalog '$id'")
  }

  def list(openApiWrap: (Page, Seq[Res]) => PagedResponse[Res],
           u: User,
           page: Option[Int],
           pageSize: Option[Int],
           catId: Opt[UUID])(_list: => Future[Seq[Res]]): Future[Result] =
    (for {
      pps <- EitherT.fromEither[Future](
        controllers.validatePageParamsWithDefault(page, pageSize)
      )
      (page, pageSize) = pps
      _ <- Semigroupal.traverse2(chkCatalogExists, catId: Option[UUID])(_(_))
      resources <- EitherT.liftF[Future, Result, Seq[Res]](
        _list.flatMap(auth.filterWithReadPermission(u, resourceType, catId, _)(extractId))
      )
    } yield Ok(openApiWrap.tupled(paginate(resources, page, pageSize)))).merge

  def find(u: User, resId: ResourceId, catId: Opt[CatalogId])(_find: => Future[Option[Res]]): Future[Result] =
    auth
      .checkPermission(u, resourceType, ActionType.Read, catId, resId)
      .flatMap(ifAuthorized(_find.map(_.fold(idNotFound(resId, catId))(Ok(_)))))

  def delete(u: User, resId: ResourceId, catId: Opt[CatalogId])(
      _delete: => Future[RepoError | Boolean]): Future[Result] =
    auth
      .checkPermission(u, resourceType, ActionType.Delete, catId, resId)
      .flatMap(ifAuthorized(_delete.map(foldDeleted((resId, catId), _))))

  def create(u: User, catalogId: Opt[CatalogId])(_create: => Future[RepoError | Res]): Future[Result] =
    (for {
      res <- EitherT(
        auth
          .checkCreatePermission(u, resourceType, catalogId)
          .flatMap(ifAuthorizedOrErr(_create)))
      _ <- EitherT[Future, PantheonError, PermissionTO](
        auth.create(extractId(res), resourceType, PermissionTO(UUID.randomUUID(), u.userId, "Owner")))
    } yield res).value.map(foldCreated(_))

  def update(u: User, resId: ResourceId, catId: Opt[CatalogId])(
      _update: => Future[RepoError | Option[Res]]): Future[Result] =
    auth
      .checkPermission(u, resourceType, ActionType.Edit, catId, resId)
      .flatMap(ifAuthorized(_update.map(_.fold(handlePantheonErr, _.fold(idNotFound((resId, catId)))(Ok(_))))))
}
