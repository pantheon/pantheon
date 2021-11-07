package services

import java.util.UUID

import config.Authentication
import enumeratum._
import errors.RepoError

import scala.concurrent.Future

object Authorization {
  sealed trait ResourceType extends EnumEntry
  // Unmanaged resources are those for which permissions are statically defined in code
  sealed trait UnmanagedResourceType extends ResourceType
  // Managed resources types are those for which permissions are checked against role actions in the db
  sealed trait ManagedResourceType extends ResourceType

  object ResourceType extends Enum[ResourceType] {
    val values = findValues

    case object BackendConfig extends UnmanagedResourceType
    case object QueryHistory extends UnmanagedResourceType

    case object Catalog extends ManagedResourceType
    case object DataSource extends ManagedResourceType
    case object DataSourceProduct extends ManagedResourceType
    case object SavedQuery extends ManagedResourceType
    case object Schema extends ManagedResourceType
  }

  object ActionType extends Enumeration {
    val Read, Edit, Delete, Grant = Value
  }

  /* TODO: use enum for role value (in case if we plan to support custom roles : Either[ProvidedRole :Enum, CustomRole:String])
   * this is needed to safely apply special logic to provided roles
   */
  case class PermissionTO(id: UUID, principalId: UUID, role: String)
}

trait Authorization {

  import Authorization._

  def checkPermission(user: Authentication.User,
                      resourceType: ResourceType,
                      action: ActionType.Value,
                      catalogId: Option[UUID],
                      resourceId: UUID): Future[Boolean]

  def checkCreatePermission(user: Authentication.User,
                            resourceType: ResourceType,
                            catalogId: Option[UUID]): Future[Boolean]

  def filterWithReadPermission[A](user: Authentication.User,
                                  resourceType: ResourceType,
                                  catalogId: Option[UUID],
                                  resources: Seq[A])(extractId: A => UUID): Future[Seq[A]]

  /*
   * CRUDL
   */

  def list(resourceId: UUID): Future[Seq[PermissionTO]]

  def find(resourceId: UUID, permissionId: UUID): Future[Option[PermissionTO]]

  def create(resourceId: UUID, resourceType: ResourceType, permission: PermissionTO): Future[RepoError | PermissionTO]

  def update(resourceId: UUID,
             resourceType: ResourceType,
             permission: PermissionTO): Future[RepoError | Option[PermissionTO]]

  def delete(resourceId: UUID, id: UUID): Future[RepoError | Boolean]

}

class AllowAllAuthorization extends Authorization {

  override def checkCreatePermission(user: Authentication.User,
                                     resourceType: Authorization.ResourceType,
                                     catalogId: Option[UUID]): Future[Boolean] = Future.successful(true)

  override def checkPermission(user: Authentication.User,
                               resourceType: Authorization.ResourceType,
                               action: Authorization.ActionType.Value,
                               catalogId: Option[UUID],
                               resourceId: UUID): Future[Boolean] = Future.successful(true)

  override def filterWithReadPermission[A](user: Authentication.User,
                                           resourceType: Authorization.ResourceType,
                                           catalogId: Option[UUID],
                                           resources: Seq[A])(p: A => UUID): Future[Seq[A]] =
    Future.successful(resources)

  override def list(resourceId: UUID): Future[Seq[Authorization.PermissionTO]] = Future.successful(Seq.empty)

  override def find(resourceId: UUID, permissionId: UUID): Future[Option[Authorization.PermissionTO]] =
    Future.successful(None)

  override def create(resourceId: UUID,
                      resourceType: Authorization.ResourceType,
                      permission: Authorization.PermissionTO): Future[RepoError | Authorization.PermissionTO] =
    // need to return successful result to facilitate no-auth scenario
    Future.successful(Right(permission))

  override def delete(resourceId: UUID, id: UUID): Future[RepoError | Boolean] = Future.successful(Right(false))

  override def update(resourceId: UUID,
                      resourceType: Authorization.ResourceType,
                      permission: Authorization.PermissionTO): Future[RepoError | Option[Authorization.PermissionTO]] =
    Future.successful(Right(Some(permission)))
}
