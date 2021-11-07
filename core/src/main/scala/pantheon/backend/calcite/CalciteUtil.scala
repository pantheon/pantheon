package pantheon.backend.calcite

import org.apache.calcite.plan.{RelOptCluster, RelOptSchema}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.tools.{FrameworkConfig, Frameworks}

import scala.annotation.tailrec

object CalciteUtil {

  @tailrec
  def rootSchema(schema: SchemaPlus): SchemaPlus =
    if (schema.getParentSchema == null) schema
    else rootSchema(schema.getParentSchema)

  def getClusterAndSchema(config: FrameworkConfig): (RelOptCluster, RelOptSchema) = {
    var cluster: RelOptCluster = null
    var schema: RelOptSchema = null
    Frameworks.withPlanner(
      new Frameworks.PlannerAction[Void]() {
        def apply(relOptCluster: RelOptCluster, relOptSchema: RelOptSchema, rootSchema: SchemaPlus): Void = {
          cluster = relOptCluster
          schema = relOptSchema
          null
        }
      },
      config
    )
    (cluster, schema)
  }
}
