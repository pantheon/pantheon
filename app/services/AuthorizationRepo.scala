package services

import java.util.UUID

import dao.Tables._
import config.Authentication
import Authorization._
import cats.data.EitherT
import errors.{ConstraintViolation, RepoError}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import util.DBIOUtil.dbioMonad
import pantheon.util.BoolOps

import scala.concurrent.{ExecutionContext, Future}

class AuthorizationRepo(implicit ec: ExecutionContext, dbConfig: DatabaseConfig[JdbcProfile]) extends Authorization {
  import dbConfig.db
  import profile.api._

  override def checkPermission(user: Authentication.User,
                               resourceType: ResourceType,
                               action: ActionType.Value,
                               catalogId: Option[UUID],
                               resourceId: UUID): Future[Boolean] = {
    resourceType match {
      case _: ManagedResourceType =>
        catalogId match {
          case Some(catId) =>
            if (user.adminCatalogIds.contains(catId)) {
              // admins of a catalog automatically have access to everything
              Future.successful(true)
            } else if (user.catalogIds.contains(catId)) {
              checkPermission(user.principalIds, action, resourceId)
            } else {
              Future.successful(false)
            }

          case _ =>
            // non-catalog-scoped resources

            Future.successful(if (resourceType == ResourceType.Catalog) {
              if (action == ActionType.Read) {
                // any user can read a catalog
                user.catalogIds.contains(resourceId)
              } else {
                // only catalog admin users can alter a catalog
                user.adminCatalogIds.contains(resourceId)
              }

            } else {
              // every user has read access; other permissions require a tenantAdmin
              (action == ActionType.Read) || user.tenantAdmin
            })
        }

      case _: UnmanagedResourceType =>
        catalogId match {
          case Some(catId) =>
            if (user.adminCatalogIds.contains(catId)) {
              // admins of a catalog have access to all resource actions
              Future.successful(true)
            } else if (user.catalogIds.contains(catId)) {
              // all users have read access
              Future.successful(action == ActionType.Read)
            } else {
              Future.successful(false)
            }

          case _ =>
            Future.successful(false)
        }
    }
  }

  override def checkCreatePermission(user: Authentication.User,
                                     resourceType: ResourceType,
                                     catalogId: Option[UUID]): Future[Boolean] = {
    resourceType match {
      case _: ManagedResourceType =>
        // 1. any user can create a catalog
        // 2. any user can create a resource within their catalog
        // 3. only tenantAdmins can create a non-catalog-scoped resource
        Future.successful(
          (resourceType == ResourceType.Catalog) || catalogId
            .exists(user.catalogIds.contains) || (catalogId.isEmpty && user.tenantAdmin))

      case _: UnmanagedResourceType =>
        Future.successful(catalogId.exists(user.adminCatalogIds.contains))
    }
  }

  override def filterWithReadPermission[A](user: Authentication.User,
                                           resourceType: ResourceType,
                                           catalogId: Option[UUID],
                                           resources: Seq[A])(extractId: A => UUID): Future[Seq[A]] = {
    resourceType match {
      case _: ManagedResourceType =>
        catalogId match {
          case Some(catId) =>
            if (user.adminCatalogIds.contains(catId)) {
              // admins of a catalog have access to everything inside
              Future.successful(resources)

            } else if (user.catalogIds.contains(catId)) {
              // user has catalog access, check explicit permissions on resources
              val filteredIds = resources.map(extractId)
              filterWithReadPermission(user.principalIds, filteredIds)
                .map(allowedIds => resources.filter(r => allowedIds.contains(extractId(r))))

            } else {
              Future.successful(Seq.empty)
            }

          case _ =>
            if (resourceType == ResourceType.Catalog) {
              // return all catalogs that a user has access to
              Future.successful(resources.filter { r =>
                user.catalogIds.contains(extractId(r))
              })

            } else {
              // non-catalog-scoped resources
              // all users have read access
              Future.successful(resources)
            }
        }

      case _: UnmanagedResourceType =>
        Future.successful(if (catalogId.exists(user.catalogIds.contains)) resources else Seq.empty)
    }
  }

  // check permission
  def checkPermission(principalIds: Seq[UUID], action: ActionType.Value, resourceId: UUID): Future[Boolean] = {
    val q = for {
      p <- PrincipalPermissions if p.resourceId === resourceId && (p.principalId inSet principalIds)
      a <- RoleActions if a.roleId === p.roleId && a.actionType === action
    } yield p
    db.run(q.exists.result)
  }

  def filterWithReadPermission(principalIds: Seq[UUID], resourceIds: Seq[UUID]): Future[Seq[UUID]] = {
    val q = for {
      p <- PrincipalPermissions if (p.resourceId inSet resourceIds) && (p.principalId inSet principalIds)
      a <- RoleActions if a.roleId === p.roleId && a.actionType === ActionType.Read
    } yield p.resourceId
    db.run(q.result)
  }

  // list permissions for resource
  override def list(resourceId: UUID): Future[Seq[PermissionTO]] = {
    val q = for {
      p <- PrincipalPermissions if p.resourceId === resourceId
      r <- Roles if r.roleId === p.roleId
    } yield (p.permissionId, p.principalId, r.name).mapTo[PermissionTO]
    db.run(q.result)
  }

  // get permission by id
  override def find(resourceId: UUID, permissionId: UUID): Future[Option[PermissionTO]] = {
    val q = for {
      p <- PrincipalPermissions if p.permissionId === permissionId
      r <- Roles if r.roleId === p.roleId
    } yield (p.permissionId, p.principalId, r.name).mapTo[PermissionTO]
    db.run(q.result.headOption)
  }

  // add single permission for resource
  override def create(resourceId: UUID,
                      resourceType: ResourceType,
                      permission: PermissionTO): Future[RepoError | PermissionTO] = {

    resourceType match {
      case _: ManagedResourceType =>
        def createPermission(roleId: UUID) =
          (PrincipalPermissions returning PrincipalPermissions) +=
            PrincipalPermissionsRow(permission.id, permission.principalId, roleId, resourceId)

        db.run(
            getRoleId(permission.role, resourceType)
              .semiflatMap(createPermission)
              .map { p =>
                assert(p.permissionId == permission.id)
                permission
              }
              .value
          )
          .detectConstraintViolation()

      case _: UnmanagedResourceType =>
        // unmanaged resource types do not keep any data in the database
        Future.successful(Right(permission))
    }
  }

  // update permission by id
  override def update(resourceId: UUID,
                      resourceType: ResourceType,
                      permission: PermissionTO): Future[RepoError | Option[PermissionTO]] = {

    def update(roleId: UUID) =
      getById(permission.id)
        .update(PrincipalPermissionsRow(permission.id, permission.principalId, roleId, resourceId))

    db.run(
        getRoleId(permission.role, resourceType)
          .semiflatMap(update)
          .map(r => (r == 1).option(permission))
          .value
      )
      .detectConstraintViolation()
  }

  // remove permission by id
  override def delete(resourceId: UUID, id: UUID): Future[RepoError | Boolean] =
    db.run(getById(id).delete.map(_ == 1)).map(Right(_))

  val getById = Compiled((id: Rep[UUID]) => PrincipalPermissions.filter(_.permissionId === id))

  private val findRoleId = Compiled((name: Rep[String], resourceType: Rep[Authorization.ResourceType]) =>
    Roles.filter(r => r.name === name && r.resourceType === resourceType).map(_.roleId))

  private def getRoleId(role: String, resourceType: Authorization.ResourceType) =
    EitherT[DBIO, ConstraintViolation, UUID](
      findRoleId(role, resourceType).result.headOption.map(_.toRight(
        ConstraintViolation(s"Error creating permission. Cannot find role with name=$role, type=$resourceType"))))
}
