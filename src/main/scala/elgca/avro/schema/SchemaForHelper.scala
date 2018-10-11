package elgca.avro.schema

import java.nio.ByteBuffer

import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.sql.{Date, Timestamp}
import java.util.UUID

import org.apache.avro.LogicalTypes.Decimal

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import scala.reflect.ClassTag

trait SchemaForHelper {

  def const[T](_schema: Schema): SchemaFor[T] = new SchemaFor[T] {
    override def schema(implicit info: MetaInfo): Schema = _schema
  }

  implicit val StringSchemaFor: SchemaFor[String] = const(SchemaBuilder.builder.stringType)
  implicit val LongSchemaFor: SchemaFor[Long] = const(SchemaBuilder.builder.longType)
  implicit val IntSchemaFor: SchemaFor[Int] = const(SchemaBuilder.builder.intType)
  implicit val DoubleSchemaFor: SchemaFor[Double] = const(SchemaBuilder.builder.doubleType)
  implicit val FloatSchemaFor: SchemaFor[Float] = const(SchemaBuilder.builder.floatType)
  implicit val BooleanSchemaFor: SchemaFor[Boolean] = const(SchemaBuilder.builder.booleanType)
  implicit val ByteArraySchemaFor: SchemaFor[Array[Byte]] = const(SchemaBuilder.builder.bytesType)
  implicit val ByteSeqSchemaFor: SchemaFor[Seq[Byte]] = const(SchemaBuilder.builder.bytesType)
  implicit val ByteListSchemaFor: SchemaFor[List[Byte]] = const(SchemaBuilder.builder.bytesType)
  implicit val ByteVectorSchemaFor: SchemaFor[Vector[Byte]] = const(SchemaBuilder.builder.bytesType)
  implicit val ByteBufferSchemaFor: SchemaFor[ByteBuffer] = const(SchemaBuilder.builder.bytesType)
  implicit val ShortSchemaFor: SchemaFor[Short] = const(SchemaBuilder.builder.intType)
  implicit val ByteSchemaFor: SchemaFor[Byte] = const(SchemaBuilder.builder.intType)

  implicit object UUIDSchemaFor extends SchemaFor[UUID] {
    override def schema(implicit info: MetaInfo): Schema = LogicalTypes.uuid().addToSchema(SchemaBuilder.builder.stringType)
  }


  implicit def mapSchemaFor[V](implicit schemaFor: SchemaFor[V]): SchemaFor[Map[String, V]] = {
    new SchemaFor[Map[String, V]] {
      override def schema(implicit info: MetaInfo): Schema = SchemaBuilder.map().values(schemaFor.schema(info))
    }
  }

  implicit object bigDecimalFor extends SchemaFor[BigDecimal] {
    override def schema(implicit info: MetaInfo): Schema = {
      val sp = info.decimalMode.getOrElse(AvroDecimalMode(8, 2))
      LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType)
    }
  }

  implicit def seqSchemaFor[T](implicit schemaFor: SchemaFor[T]): SchemaFor[Seq[T]] = {
    new SchemaFor[Seq[T]] {
      override def schema(implicit info: MetaInfo): Schema = Schema.createArray(schemaFor.schema(info))
    }
  }

  implicit def setSchemaFor[T](implicit schemaFor: SchemaFor[T]): SchemaFor[Set[T]] = {
    new SchemaFor[Set[T]] {
      override def schema(implicit info: MetaInfo): Schema = Schema.createArray(schemaFor.schema(info))
    }
  }

  implicit def vectorSchemaFor[S](implicit schemaFor: SchemaFor[S]): SchemaFor[Vector[S]] = {
    new SchemaFor[Vector[S]] {
      override def schema(implicit info: MetaInfo): Schema = Schema.createArray(schemaFor.schema(info))
    }
  }


  implicit def iterableSchemaFor[S](implicit schemaFor: SchemaFor[S]): SchemaFor[Iterable[S]] = {
    new SchemaFor[Iterable[S]] {
      override def schema(implicit info: MetaInfo): Schema = Schema.createArray(schemaFor.schema(info))
    }
  }

  implicit object TimestampSchemaFor extends SchemaFor[Timestamp] {
    override def schema(implicit info: MetaInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
  }

  implicit object LocalTimeSchemaFor extends SchemaFor[LocalTime] {
    override def schema(implicit info: MetaInfo): Schema = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder.intType)
  }

  implicit object LocalDateSchemaFor extends SchemaFor[LocalDate] {
    override def schema(implicit info: MetaInfo): Schema = LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType)
  }

  implicit object LocalDateTimeSchemaFor extends SchemaFor[LocalDateTime] {
    override def schema(implicit info: MetaInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
  }

  implicit object DateSchemaFor extends SchemaFor[java.sql.Date] {
    override def schema(implicit info: MetaInfo): Schema = LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType)
  }

  implicit object InstantSchemaFor extends SchemaFor[Instant] {
    override def schema(implicit info: MetaInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
  }


  implicit def javaEnumSchemaFor[E <: Enum[_]](implicit tag: ClassTag[E]): SchemaFor[E] = new SchemaFor[E] {
    override def schema(implicit info: MetaInfo): Schema = {
      val annos = tag.runtimeClass.getAnnotations.toList
      val extractor = MetaInfo(annos)
      val name = tag.runtimeClass.getSimpleName
      val namespace = extractor.namespace.getOrElse(tag.runtimeClass.getPackage.getName)
      val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)
      SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
    }
  }

  //UNION
  implicit def optionSchemaFor[T](implicit schemaFor: SchemaFor[T]): SchemaFor[Option[T]] = new SchemaFor[Option[T]] {
    override def schema(implicit info: MetaInfo): Schema = {
      val elementSchema = schemaFor.schema(info)
      val nullSchema = SchemaBuilder.builder().nullType()
      createSafeUnion(elementSchema, nullSchema)
    }
  }

  def createSafeUnion(schemas: Schema*): Schema = {
    val flattened = schemas.flatMap {
      x =>
        if (x.getType == Schema.Type.UNION)
          x.getTypes.asScala
        else
          Seq(x)
    }
    val (nulls, rest) = flattened.partition(_.getType == Schema.Type.NULL)
    Schema.createUnion(nulls.headOption.toSeq ++ rest: _*)
  }
}
