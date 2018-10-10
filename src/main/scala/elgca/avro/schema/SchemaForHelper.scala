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

  trait SchemaForRight[T] extends SchemaFor[T] {

    override def schema(implicit info: AnnotationInfo): Schema

    override def write(t: T): Either[Record, AnyRef] = Right(right(t))

    def right(t: T): AnyRef
  }

  def const[@specialized T](_schema: Schema, f: T => AnyRef): SchemaFor[T] = new SchemaForRight[T] {
    override def schema(implicit info: AnnotationInfo): Schema = _schema

    override def right(t: T): AnyRef = f(t)
  }

  implicit val StringSchemaFor: SchemaFor[String] = const(SchemaBuilder.builder.stringType, identity)
  implicit val LongSchemaFor: SchemaFor[Long] = const(SchemaBuilder.builder.longType, java.lang.Long.valueOf)
  implicit val IntSchemaFor: SchemaFor[Int] = const(SchemaBuilder.builder.intType, java.lang.Integer.valueOf)
  implicit val DoubleSchemaFor: SchemaFor[Double] = const(SchemaBuilder.builder.doubleType, java.lang.Double.valueOf)
  implicit val FloatSchemaFor: SchemaFor[Float] = const(SchemaBuilder.builder.floatType, java.lang.Float.valueOf)
  implicit val BooleanSchemaFor: SchemaFor[Boolean] = const(SchemaBuilder.builder.booleanType, java.lang.Boolean.valueOf)
  implicit val ByteArraySchemaFor: SchemaFor[Array[Byte]] = const(SchemaBuilder.builder.bytesType, ByteBuffer.wrap)
  val seqWrap = (seq: Seq[Byte]) => ByteBuffer.wrap(seq.toArray)
  implicit val ByteSeqSchemaFor: SchemaFor[Seq[Byte]] = const(SchemaBuilder.builder.bytesType, seqWrap)
  implicit val ByteListSchemaFor: SchemaFor[List[Byte]] = const(SchemaBuilder.builder.bytesType, seqWrap)
  implicit val ByteVectorSchemaFor: SchemaFor[Vector[Byte]] = const(SchemaBuilder.builder.bytesType, seqWrap)
  implicit val ByteBufferSchemaFor: SchemaFor[ByteBuffer] = const(SchemaBuilder.builder.bytesType, identity)
  implicit val ShortSchemaFor: SchemaFor[Short] = const(SchemaBuilder.builder.intType, t => java.lang.Integer.valueOf(t))
  implicit val ByteSchemaFor: SchemaFor[Byte] = const(SchemaBuilder.builder.intType, t => java.lang.Integer.valueOf(t))

  implicit object UUIDSchemaFor extends SchemaForRight[UUID] {
    override def right(t: UUID): AnyRef = t.toString

    override def schema(implicit info: AnnotationInfo): Schema = LogicalTypes.uuid().addToSchema(SchemaBuilder.builder.stringType)
  }

  def writeHelper[T](schemaFor: SchemaFor[T], t: T): AnyRef = {
    schemaFor.write(t) match {
      case Left(v) => v
      case Right(v) => v
    }
  }

  implicit def mapSchemaFor[V](implicit schemaFor: SchemaFor[V]): SchemaFor[Map[String, V]] = {
    new SchemaForRight[Map[String, V]] {

      override def right(t: Map[String, V]): AnyRef = {
        t.mapValues(x => writeHelper(schemaFor, x)).toIterator.toMap.asJava
      }

      override def schema(implicit info: AnnotationInfo): Schema = SchemaBuilder.map().values(schemaFor.schema)
    }
  }

  implicit object bigDecimalFor extends SchemaForRight[BigDecimal] {
    override def schema(implicit info: AnnotationInfo): Schema = {
      val sp = info.decimalMode.getOrElse(AvroDecimalMode(8, 2))
      LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType)
    }

    override def right(t: BigDecimal): AnyRef = {
      /*schema.getType match {
        case Schema.Type.STRING => t.toString()
        case Schema.Type.BYTES =>
          val decimal = schema.getLogicalType.asInstanceOf[Decimal]
          require(decimal != null)
          val decimalConversion = new Conversions.DecimalConversion
          val decimalType = LogicalTypes.decimal(decimal.getPrecision, decimal.getScale)
          val scaledValue = t.setScale(decimal.getScale, RoundingMode.HALF_UP)
          val bytes = decimalConversion.toBytes(scaledValue.bigDecimal, null, decimalType)
          bytes
        case Schema.Type.FIXED => sys.error("Unsupported. PR Please!")
        case _ => sys.error(s"Cannot serialize BigDecimal as ${schema.getType}")
      }*/
      ???
    }
  }

  implicit def seqSchemaFor[T](implicit schemaFor: SchemaFor[T]): SchemaFor[Seq[T]] = {
    new SchemaForRight[Seq[T]] {
      override def schema(implicit info: AnnotationInfo): Schema = Schema.createArray(schemaFor.schema)

      override def right(t: Seq[T]): AnyRef = t.map(x => writeHelper(schemaFor, x)).asJava
    }
  }

  implicit def setSchemaFor[T](implicit schemaFor: SchemaFor[T]): SchemaFor[Set[T]] = {
    new SchemaForRight[Set[T]] {
      override def right(t: Set[T]): AnyRef = t.map(x => writeHelper(schemaFor, x)).asJava

      override def schema(implicit info: AnnotationInfo): Schema = Schema.createArray(schemaFor.schema)
    }
  }

  implicit def vectorSchemaFor[S](implicit schemaFor: SchemaFor[S]): SchemaFor[Vector[S]] = {
    new SchemaForRight[Vector[S]] {
      override def right(t: Vector[S]): AnyRef = t.map(x => writeHelper(schemaFor, x)).asJava

      override def schema(implicit info: AnnotationInfo): Schema = Schema.createArray(schemaFor.schema)
    }
  }


  implicit def iterableSchemaFor[S](implicit schemaFor: SchemaFor[S]): SchemaFor[Iterable[S]] = {
    new SchemaForRight[Iterable[S]] {
      override def right(t: Iterable[S]): AnyRef = t.map(x => writeHelper(schemaFor, x)).toList.asJava

      override def schema(implicit info: AnnotationInfo): Schema = Schema.createArray(schemaFor.schema)
    }
  }

  implicit object TimestampSchemaFor extends SchemaForRight[Timestamp] {
    override def right(t: Timestamp): AnyRef = ???

    override def schema(implicit info: AnnotationInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)
  }

  implicit object LocalTimeSchemaFor extends SchemaForRight[LocalTime] {
    override def schema(implicit info: AnnotationInfo): Schema = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder.intType)

    override def right(t: LocalTime): AnyRef = ???
  }

  implicit object LocalDateSchemaFor extends SchemaForRight[LocalDate] {
    override def schema(implicit info: AnnotationInfo): Schema = LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType)

    override def right(t: LocalDate): AnyRef = ???
  }

  implicit object LocalDateTimeSchemaFor extends SchemaForRight[LocalDateTime] {
    override def schema(implicit info: AnnotationInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)

    override def right(t: LocalDateTime): AnyRef = ???
  }

  implicit object DateSchemaFor extends SchemaForRight[java.sql.Date] {
    override def schema(implicit info: AnnotationInfo): Schema = LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType)

    override def right(t: Date): AnyRef = ???
  }

  implicit object InstantSchemaFor extends SchemaForRight[Instant] {
    override def schema(implicit info: AnnotationInfo): Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)

    override def right(t: Instant): AnyRef = ???
  }


  implicit def javaEnumSchemaFor[E <: Enum[_]](implicit tag: ClassTag[E]): SchemaFor[E] = new SchemaForRight[E] {
    override def schema(implicit info: AnnotationInfo): Schema = {
      val annos = tag.runtimeClass.getAnnotations.toList
      val extractor = AnnotationInfo(annos)
      val name = tag.runtimeClass.getSimpleName
      val namespace = extractor.namespace.getOrElse(tag.runtimeClass.getPackage.getName)
      val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)
      SchemaBuilder.enumeration(name).namespace(namespace).symbols(symbols: _*)
    }

    override def right(t: E): AnyRef = ???
  }

  //UNION
  implicit def optionSchemaFor[T](implicit schemaFor: SchemaFor[T]): SchemaFor[Option[T]] = new SchemaForRight[Option[T]] {
    override def schema(implicit info: AnnotationInfo): Schema = {
      val elementSchema = schemaFor.schema
      val nullSchema = SchemaBuilder.builder().nullType()
      createSafeUnion(elementSchema, nullSchema)
    }

    override def right(t: Option[T]): AnyRef = ???
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
