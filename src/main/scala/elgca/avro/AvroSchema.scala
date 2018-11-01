package elgca.avro

import java.nio.ByteBuffer

import org.apache.avro.{Schema, SchemaBuilder}

import scala.language.experimental.macros
import scala.language.higherKinds

trait AvroSchema[T] {
  def schema: Schema
}

trait AvroSchemaImplicit {

  @inline private def const[T](_schema: Schema): AvroSchema[T] = new AvroSchema[T] {
    override lazy val schema: Schema = _schema
  }

  //primary types
  final implicit val StringSchemaFor: AvroSchema[String] = const(SchemaBuilder.builder.stringType)
  final implicit val LongSchemaFor: AvroSchema[Long] = const(SchemaBuilder.builder.longType)
  final implicit val IntSchemaFor: AvroSchema[Int] = const(SchemaBuilder.builder.intType)
  final implicit val DoubleSchemaFor: AvroSchema[Double] = const(SchemaBuilder.builder.doubleType)
  final implicit val FloatSchemaFor: AvroSchema[Float] = const(SchemaBuilder.builder.floatType)
  final implicit val BooleanSchemaFor: AvroSchema[Boolean] = const(SchemaBuilder.builder.booleanType)
  final implicit val ByteBufferSchemaFor: AvroSchema[ByteBuffer] = const(SchemaBuilder.builder.bytesType)
  final implicit val arrayBytesSchemaFor: AvroSchema[Array[Byte]] = const(SchemaBuilder.builder.bytesType)
  final implicit val ShortSchemaFor: AvroSchema[Short] = const(SchemaBuilder.builder.intType)
  final implicit val ByteSchemaFor: AvroSchema[Byte] = const(SchemaBuilder.builder.intType)
  /*
    //  implicit object bigDecimalAvro extends AvroSchema[BigDecimal] {
    //    override lazy val schema: Schema = {
    //      val sp = info.decimalMode.getOrElse(AvroDecimalMode(8, 2))
    //      LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType)
    //    }
    //  }

    //  implicit def bigDecimalFor(implicit mode: AvroDecimalMode): AvroSchema[BigDecimal] = new AvroSchema[BigDecimal] {
    //    override def schema(metaInfo: MetaInfo): Schema = {
    //      val ps = Option(metaInfo).flatMap(_.decimalMode).getOrElse(mode)
    //      LogicalTypes.decimal(ps.precision, ps.scale).addToSchema(SchemaBuilder.builder.bytesType)
    //    }
    //  }


    implicit object UUIDAvroSchema extends AvroSchema[UUID] {
      override lazy val schema: Schema = LogicalTypes.uuid().addToSchema(SchemaBuilder.builder.stringType)
    }

    //timestamp support
    implicit object TimestampAvroSchema extends AvroSchema[Timestamp] {
      override lazy val schema: Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
    }

    implicit object LocalTimeAvroSchema extends AvroSchema[LocalTime] {
      override lazy val schema: Schema = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder.intType)
    }

    implicit object LocalDateAvroSchema extends AvroSchema[LocalDate] {
      override lazy val schema: Schema = LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType)
    }

    implicit object LocalDateTimeAvroSchema extends AvroSchema[LocalDateTime] {
      override lazy val schema: Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
    }

    implicit object DateAvroSchema extends AvroSchema[java.sql.Date] {
      override lazy val schema: Schema = LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType)
    }

    implicit object InstantAvroSchema extends AvroSchema[Instant] {
      override lazy val schema: Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
    }*/

//  implicit def javaEnumSchemaFor[E <: Enum[_]](implicit tag: ClassTag[E]): AvroSchema[E] = new AvroSchema[E] {
//    override lazy val schema: Schema = {
//      val annos = tag.runtimeClass.getAnnotations.toList
//      val extractor = MetaInfo(annos)
//      val name = tag.runtimeClass.getSimpleName
//      val namespace = extractor.namespace.getOrElse(tag.runtimeClass.getPackage.getName)
//      val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)
//      SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
//    }
//  }
}
