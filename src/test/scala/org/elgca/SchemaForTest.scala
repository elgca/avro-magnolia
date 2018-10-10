package org.elgca

import java.sql.Date

import org.apache.avro.Schema
import org.elgca.avro.schema.SchemaFor

sealed trait Operate

case object Download extends Operate

case object Upload extends Operate

case object Cmd extends Operate

case class Sub(name: String, sugar: Double, fat: Double)

case class TestClass(name: String,
                 opt: Date,
                 binary: List[Byte],
                 ingredients: Vector[Sub],
                 vegetarian: Boolean,
                 vegan: Boolean,
                 calories: Int)

object SchemaForTest extends App {

  import org.elgca.avro.schema.SchemaForImp._

  val schema = SchemaFor.gen[TestClass]
  println(schema.schema)
  val schema2 = new Schema.Parser().parse(
    schema.schema.toString
  )
  println(schema2)
}
