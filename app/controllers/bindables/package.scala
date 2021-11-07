package controllers

import play.api.mvc.PathBindable
package object bindables {
  implicit def somePathBindable[T](implicit p: PathBindable[T]): PathBindable[Some[T]] = p.transform(Some(_), _.get)
}
