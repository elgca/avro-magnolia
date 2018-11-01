package elgca


import java.nio.ByteBuffer

import elgca.avro.{Avro4s, Avro4sFactory, AvroDecimalMode, AvroDecoder, AvroDecoderImplicit, AvroEncoder, AvroEncoderImplicit, AvroSchema, AvroSchemaImplicit, NamingStrategy}
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.language.higherKinds
import scala.util.Try

trait Implicit {
  implicit def iteratorHandler[T, CC[_]](implicit ev$1: CC[T] => Iterable[T],
                                         avro4sFactory: Avro4sFactory[T],
                                         cbf: CanBuildFrom[Nothing, T, CC[T]]): Avro4sFactory[CC[T]] = {
    new Avro4sFactory[CC[T]] {
      fromFactory(avro4sFactory)

      override def build(): Avro4s[CC[T]] = new Avro4s[CC[T]] {
        private val avro4s = avro4sFactory.getOrCreate()

        override def encode(t: CC[T]): AnyRef = {
          t.toIterator.map(avro4s.encode).toList.asJava
        }

        override def schema: Schema = Schema.createArray(avro4s.schema)

        override def decode(anyRef: AnyRef): CC[T] = {
          anyRef match {
            case array: Array[_] => array.toVector.map(x => avro4s.decode(x.asInstanceOf[AnyRef])).to[CC]
            case list: java.util.Collection[_] => list.asScala.map(x => avro4s.decode(x.asInstanceOf[AnyRef])).to[CC]
            case other => sys.error("Unsupported vector " + other)
          }
        }
      }
    }
  }

  implicit def bytesHandler[CC[_]](implicit ev$1: CC[Byte] => Iterable[Byte],
                                   avro4sFactory: Avro4sFactory[Array[Byte]],
                                   cbf: CanBuildFrom[Nothing, Byte, CC[Byte]]): Avro4sFactory[CC[Byte]] = {
    new Avro4sFactory[CC[Byte]] {
      fromFactory(avro4sFactory)
      override def build(): Avro4s[CC[Byte]] = new Avro4s[CC[Byte]] {
        private val avro4s = avro4sFactory.getOrCreate()

        override def encode(t: CC[Byte]): AnyRef = avro4s.encode(t.toArray)

        override def schema: Schema = avro4s.schema

        override def decode(anyRef: AnyRef): CC[Byte] = avro4s.decode(anyRef).to[CC]
      }
    }
  }

  implicit def mapHandler[T, CC](implicit ev$1: CC => scala.collection.Map[String, T],
                                 avro4sFactory: Avro4sFactory[T],
                                 bf: CanBuildFrom[Map[String, T], (String, T), CC]): Avro4sFactory[CC] = {
    new Avro4sFactory[CC] {
      fromFactory(avro4sFactory)

      override def build(): Avro4s[CC] = new Avro4s[CC] {
        private val avro4s = avro4sFactory.getOrCreate()

        override def encode(t: CC): AnyRef = {
          t.mapValues(x => {
            avro4s.encode(x)
          }).asJava
        }

        override def schema: Schema = SchemaBuilder.map().values(avro4s.schema)

        override def decode(anyRef: AnyRef): CC = {
          anyRef match {
            case array: java.util.Map[String, AnyRef] =>
              val repr = array.asScala.mapValues(avro4s.decode).toMap
              val b = bf(repr)
              b.sizeHint(repr)
              for (x <- repr) b += x
              b.result
            case other => sys.error("Unsupported vector " + other)
          }
        }
      }
    }
  }

  implicit def baseHandler[T](implicit handle: AvroSchema[T], encoder: AvroEncoder[T], decoder: AvroDecoder[T]): Avro4sFactory[T] = {
    new Avro4sFactory[T] {
      override def build(): Avro4s[T] = new Avro4s[T] {
        override def encode(t: T): AnyRef = encoder.encode(t)

        override def schema: Schema = handle.schema

        override def decode(anyRef: AnyRef): T = decoder.decode(anyRef)
      }
    }
  }

  implicit def optionHandler[T](implicit factory: Avro4sFactory[T]): Avro4sFactory[Option[T]] = {
    type nullAble = Option[T]
    new Avro4sFactory[nullAble] {
      fromFactory(factory)
      override def build(): Avro4s[nullAble] = new Avro4s[nullAble] {
        override def encode(t: nullAble): AnyRef = {
          t.map(factory.getOrCreate().encode).orNull
        }

        override def schema: Schema = {
          Avro4sFactory.createSafeUnion(SchemaBuilder.builder.nullType(), factory.getOrCreate().schema)
        }

        override def decode(anyRef: AnyRef): nullAble = Option(anyRef).map(factory.getOrCreate().decode)
      }
    }
  }

  implicit def decimalHandler(implicit avroDecimalMode: AvroDecimalMode): Avro4sFactory[BigDecimal] = new Avro4sFactory[BigDecimal] {
    self =>
    override def build(): Avro4s[BigDecimal] = new Avro4s[BigDecimal] {
      private val sp = Option(self._decimalMode).getOrElse(avroDecimalMode)
      override val schema: Schema = {
        LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType)
      }

      override def decode(anyRef: AnyRef): BigDecimal = {
        val decimalConversion = new Conversions.DecimalConversion
        val decimalType = LogicalTypes.decimal(sp.precision, sp.scale)
        val bytes = anyRef.asInstanceOf[ByteBuffer]
        decimalConversion.fromBytes(bytes, null, decimalType)
      }

      override def encode(t: BigDecimal): AnyRef = {
        schema.getType match {
          case Schema.Type.BYTES =>
            val decimalType = LogicalTypes.decimal(sp.precision, sp.scale)
            val decimalConversion = new Conversions.DecimalConversion
            val scaledValue = t.setScale(sp.scale, sp.roundingMode)
            decimalConversion.toBytes(scaledValue.bigDecimal, null, decimalType)
          case _ => sys.error(s"Cannot serialize BigDecimal as ${schema.getType}")
        }
      }
    }
  }

  implicit def eitherHandler[A, B](implicit a: Avro4sFactory[A], b: Avro4sFactory[B]): Avro4sFactory[Either[A, B]] = {
    type either = Either[A, B]
    new Avro4sFactory[either] {
      override def build(): Avro4s[either] = new Avro4s[either] {
        override def encode(t: either): AnyRef = {
          t match {
            case Left(l) => a.getOrCreate().encode(l)
            case Right(r) => b.getOrCreate().encode(r)
          }
        }

        override def schema: Schema = {
          Avro4sFactory.createSafeUnion(a.getOrCreate().schema, b.getOrCreate().schema)
        }

        override def decode(value: AnyRef): either = {
          Try(a.getOrCreate().decode(value)).map(Left[A, B]).getOrElse(
            Try(b.getOrCreate().decode(value)).map(Right[A, B]).get
          )
        }
      }
    }
  }
}

package object avro
  extends Implicit
    with AvroDecimalModeImplicit
    with AvroDecoderImplicit
    with AvroEncoderImplicit
    with AvroSchemaImplicit