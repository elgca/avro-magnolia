package elgca.avro.schema

import java.nio.ByteBuffer
import java.sql.Date
import java.time.LocalTime
import java.util.UUID

import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.language.higherKinds


sealed trait Operate

case object Download extends Operate

case object Upload extends Operate

case object Cmd extends Operate

sealed trait Human

case class Man(sex: String, ogg: String) extends Human

case class Woman(sex: String, manko: String) extends Human

case object NullHuman extends Human

case object Unkones extends Human

case class Sub(name: String, sugar: Map[String, String], fat: Operate)

@AvroName("hello")
case class TestClass(name: (Int, Double),
                     @AvroDecimalMode(38, 10)
                     opt: Either[String, BigDecimal],
                     binary: Vector[Byte],
                     oth: Option[Human],
                    )

case class Test2(opt: String,
                 int: Int,
                 double: Double,
                 uuid: UUID,
                 buf: ByteBuffer,
                 array: Array[Byte],
                 seq: List[String])

object Avro4sTest extends App {

  import Avro4s._

  implicit val naming = SnakeCase

  implicit def baseHandler[T](implicit schemaFor: AvroSchema[T]): AvroDerivation[T] =
    AvroDerivation[T](schemaFor.schema, null)

  implicit def bytesHandler[CC[_]](implicit ev$1: CC[Byte] => Iterable[Byte],
                                   schemaFor: AvroSchema[Array[Byte]]): AvroDerivation[CC[Byte]] = {
    AvroDerivation[CC[Byte]](schemaFor.schema, null)
  }

  implicit def vectorHandler[T, CC[_]](implicit ev$1: CC[T] => Iterable[T],
                                       handle: AvroDerivation[T]): AvroDerivation[CC[T]] = {
    AvroDerivation[CC[T]](info => Schema.createArray(handle.schema(info)), null)
  }

  implicit def mapHandler[T, CC[String, _]](implicit ev$1: CC[String, T] => Map[String, T],
                                            handle: AvroDerivation[T]): Typeclass[CC[String, T]] = {
    AvroDerivation[CC[String, T]](info => SchemaBuilder.map().values(handle.schema(info)), null)
  }

  implicit def optionHandler[T](implicit handle: AvroDerivation[T]): AvroDerivation[Option[T]] = {
    def schema(info: MetaInfo): Schema = {
      val elementSchema = handle.schema(info)
      val nullSchema = SchemaBuilder.builder().nullType()
      createSafeUnion(elementSchema, nullSchema)
    }

    AvroDerivation(schema, null)
  }

  implicit def eitherHandler[A, B](implicit a: AvroSchema[A], b: AvroSchema[B]): AvroDerivation[Either[A, B]] = {
    def schema(info: MetaInfo): Schema = {
      val left = a.schema(info)
      val right = b.schema(info)
      createSafeUnion(left, right)
    }

    AvroDerivation(schema, null)
  }

  val gens = gen[Test2]
  implicit val empty = MetaInfo.Empty
  val schema = gens.schema(empty)

  println(schema)
  val schema2 = new Schema.Parser().parse(
    gens.schema(empty).toString
  )

  //  new GenericData.Record(schema)
  //  println(schema2)
  println(schema.toString == schema2.toString)
}
