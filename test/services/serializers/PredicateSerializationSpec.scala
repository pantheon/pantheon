package services.serializers
import org.scalatest.{MustMatchers, WordSpec}
import pantheon.planner.Predicate
import play.api.libs.json.{JsString, JsSuccess, Json}
import QuerySerializers.predicateFormat
import pantheon.planner.ASTBuilders._

class PredicateSerializationSpec extends WordSpec  {
  "Predicate serializer" must {
      "(de)serialize predicate symmetrically" in {
        // TODO: support or in parser
        val serializedPredicate = JsString("a > 3 and b.c < :ref and z in ('A', 'B') and x >= 1 and y <= 2 and z = 3 and f != 0 and j not in ('x')")
        val predicate = ref("a") > lit(3) &
          ref("b.c") < bindVar("ref") &
          ref("z").in(lit("A"), lit("B")) &
          ref("x") >=(lit(1)) &
          ref("y") <=(lit(2)) &
          ref("z") === lit(3) &
          ref("f").!==(lit(0)) &
          ref("j").notIn(lit("x"))

        assert(serializedPredicate.validate[Predicate] == JsSuccess(predicate))

        assert(Json.toJson(predicate) == serializedPredicate)
      }
    }
  }
