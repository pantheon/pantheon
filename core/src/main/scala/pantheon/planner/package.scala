package pantheon

import pantheon.planner.Planner.Rule

package object planner {

  def substituteBindVariables(params: Map[String, Expression]): Rule = _ transformUp {
    case BindVariable(name) => params(name)
  }

}
