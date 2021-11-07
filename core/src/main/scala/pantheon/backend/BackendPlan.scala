package pantheon.backend

trait BackendPlan {
  def canonical: String = toString
}
