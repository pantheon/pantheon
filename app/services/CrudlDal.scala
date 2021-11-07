package services

import scala.concurrent.Future
import scala.util.Try

// Delete the following classes/traits if they are not used/needed
trait ReadNestedResource[T] {
  def list(parentId: Int): Future[Seq[T]]
  def find(parentId: Int, id: Int): Future[Option[T]]
}

trait ReadNested2Resource[T] {
  def list(parentId: Int, subparentId: Int): Future[Seq[T]]
  def find(parentId: Int, subparentId: Int, id: Int): Future[Option[T]]
}

trait NestedResource[T, U, C] {
  def list(parentId: Int): Future[Seq[T]]
  def find(parentId: Int, id: Int): Future[Option[T]]
  def destroy(parentId: Int, id: Int): Future[Try[String]]
  def update(parentId: Int, id: Int, link: U): Future[Try[T]]
  def create(parentId: Int, link: C): Future[Try[T]]
}

case class InvalidResource(id: String) extends Exception(id)
case class InvalidResourceField(id: Option[String], field: String, error: String) extends Exception
case class ResultNotFound(msg: String) extends Exception(msg)
