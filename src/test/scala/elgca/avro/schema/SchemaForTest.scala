package elgca.avro.schema

import java.sql.Date

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}


sealed trait Operate

case object Download extends Operate

case object Upload extends Operate

case object Cmd extends Operate

sealed trait Human
case class Man(sex:String,ogg:String) extends Human
case class Woman(sex:String,manko:String) extends Human

case class Sub(name: String, sugar: Map[String,String], fat: Operate)

@AvroName("hello")
case class TestClass(name: (Int,Double),
                     @AvroDecimalMode(38,10)
                     either: Either[BigDecimal,String],
                     opt: Human,
                     binary: List[Byte],
                     Subs: Vector[Sub],
                     vegan: Date,
                     colorEnum: ColorEnum,
                     @AvroDecimalMode(38,10)
                     decml: BigDecimal)

object SchemaForTest extends App {

  import SchemaFor._
  implicit val naming = SnakeCase
  val gens = gen[TestClass]
  implicit val empty = MetaInfo.Empty
  val schema = gens.schema
  println(schema)
  val schema2 = new Schema.Parser().parse(
    gens.schema.toString
  )

  new GenericData.Record(schema)
  println(schema2)
  println(schema.toString == schema2.toString)
  trait Record extends GenericRecord with SpecificRecord

  new Record {
    override def put(key: String, v: scala.Any): Unit = ???

    override def put(i: Int, v: scala.Any): Unit = ???

    override def get(key: String): AnyRef = ???

    override def get(i: Int): AnyRef = ???

    override def getSchema: Schema = ???
  }
}
