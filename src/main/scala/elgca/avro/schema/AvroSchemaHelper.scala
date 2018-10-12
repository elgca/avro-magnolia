package elgca.avro.schema

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.language.higherKinds
import scala.reflect.ClassTag

trait AvroSchemaHelper {

  def const[T](_schema: Schema): AvroSchema[T] = new AvroSchema[T] {
    override def schema(info: MetaInfo): Schema = _schema
  }

  //primary types
  implicit val StringSchemaFor: AvroSchema[String] = const(SchemaBuilder.builder.stringType)
  implicit val LongSchemaFor: AvroSchema[Long] = const(SchemaBuilder.builder.longType)
  implicit val IntSchemaFor: AvroSchema[Int] = const(SchemaBuilder.builder.intType)
  implicit val DoubleSchemaFor: AvroSchema[Double] = const(SchemaBuilder.builder.doubleType)
  implicit val FloatSchemaFor: AvroSchema[Float] = const(SchemaBuilder.builder.floatType)
  implicit val BooleanSchemaFor: AvroSchema[Boolean] = const(SchemaBuilder.builder.booleanType)
  implicit val ByteBufferSchemaFor: AvroSchema[ByteBuffer] = const(SchemaBuilder.builder.bytesType)
  implicit val ShortSchemaFor: AvroSchema[Short] = const(SchemaBuilder.builder.intType)
  implicit val ByteSchemaFor: AvroSchema[Byte] = const(SchemaBuilder.builder.intType)

  implicit object bigDecimalAvro extends AvroSchema[BigDecimal] {
    override def schema(info: MetaInfo): Schema = {
      val sp = info.decimalMode.getOrElse(AvroDecimalMode(8, 2))
      LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType)
    }
  }

  implicit object UUIDAvroSchema extends AvroSchema[UUID] {
    override def schema(info: MetaInfo): Schema = LogicalTypes.uuid().addToSchema(SchemaBuilder.builder.stringType)
  }

  //timestamp support
  implicit object TimestampAvroSchema extends AvroSchema[Timestamp] {
    override def schema(info: MetaInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
  }

  implicit object LocalTimeAvroSchema extends AvroSchema[LocalTime] {
    override def schema(info: MetaInfo): Schema = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder.intType)
  }

  implicit object LocalDateAvroSchema extends AvroSchema[LocalDate] {
    override def schema(info: MetaInfo): Schema = LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType)
  }

  implicit object LocalDateTimeAvroSchema extends AvroSchema[LocalDateTime] {
    override def schema(info: MetaInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
  }

  implicit object DateAvroSchema extends AvroSchema[java.sql.Date] {
    override def schema(info: MetaInfo): Schema = LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType)
  }

  implicit object InstantAvroSchema extends AvroSchema[Instant] {
    override def schema(info: MetaInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
  }

  implicit def javaEnumSchemaFor[E <: Enum[_]](implicit tag: ClassTag[E]): AvroSchema[E] = new AvroSchema[E] {
    override def schema(info: MetaInfo): Schema = {
      val annos = tag.runtimeClass.getAnnotations.toList
      val extractor = MetaInfo(annos)(info)
      val name = tag.runtimeClass.getSimpleName
      val namespace = extractor.namespace.getOrElse(tag.runtimeClass.getPackage.getName)
      val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)
      SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
    }
  }
}
