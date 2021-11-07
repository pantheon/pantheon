package pantheon.planner

trait TreeNode[T <: TreeNode[T]] extends Product {
  self: T =>

  def fastEq(t: TreeNode[_]): Boolean = this.eq(t) || this == t

  def size: Int = 1 + children.map(_.size).sum

  def children: Iterator[T] = {
    productIterator.foldLeft(Iterator.empty.asInstanceOf[Iterator[T]]) { (l, r) =>
      r match {
        case children: Traversable[T] if children.nonEmpty && children.head.isInstanceOf[T] => l ++ children
        case t: TreeNode[T]                                                                 => l ++ Iterator.single(t.asInstanceOf[T])
        case _: Any                                                                         => l
      }
    }
  }

  def foreachDown(b: T => Unit): Unit = {
    b(this)
    children.foreach(_.foreachDown(b))
  }

  def foreachUp(b: T => Unit): Unit = {
    children.foreach(_.foreachUp(b))
    b(this)
  }

  def collect[B](pf: PartialFunction[T, B]): List[B] = {
    val list = children.foldLeft(List.empty[B]) { (l, c) =>
      c.collect(pf) ::: l
    }
    if (pf.isDefinedAt(this)) pf(this) :: list else list
  }

  def collectFirst[B](pf: PartialFunction[T, B]): Option[B] = {
    if (pf.isDefinedAt(this)) Some(pf(this))
    else
      children.foldLeft(Option.empty[B]) { (opt, c) =>
        opt.orElse(c.collectFirst(pf))
      }
  }

  def find(p: T => Boolean): Option[T] = {
    if (p(this)) Some(this)
    else children.collectFirst { case c: TreeNode[T] if p(c.asInstanceOf[T]) => c.asInstanceOf[T] }
  }

  def transformDown(rule: PartialFunction[T, T]): T = {
    val newValue = rule.applyOrElse(this, identity[T])

    if (this fastEq newValue) mapChildren(_.transformDown(rule))
    else newValue.mapChildren(_.transformDown(rule))
  }

  def transformUp(rule: PartialFunction[T, T]): T = {
    val newValue = mapChildren(_.transformUp(rule))
    if (this fastEq newValue) rule.applyOrElse(this, identity[T])
    else rule.applyOrElse(newValue, identity[T])
  }

  // we are expecting that all children shall be of type T and either provided as a
  // param directly or
  def mapChildren(f: T => T): T = {
    var changed = false
    val newValues = productIterator.map {
      case children: Traversable[T] if children.nonEmpty && children.head.isInstanceOf[T] =>
        children.map { c =>
          val newChild = f(c)
          if (!newChild.fastEq(c)) changed = true
          newChild
        }
      case child: TreeNode[_] =>
        val newChild = f(child.asInstanceOf[T])
        if (!newChild.fastEq(child)) changed = true
        newChild
      case other: AnyRef => other
      case null          => null
    }.toArray
    if (changed) makeCopy(newValues) else this
  }

  // TODO Exception handling?
  def makeCopy(newValues: Array[AnyRef]): T = {
    val copyMethod = this.getClass.getMethods.find(_.getName == "copy").get
    copyMethod.invoke(this, newValues: _*).asInstanceOf[T]
  }
}
