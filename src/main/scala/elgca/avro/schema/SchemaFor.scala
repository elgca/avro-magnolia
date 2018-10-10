package elgca.avro.schema

import java.util.{UUID, List => JList, Map => JMap}

import magnolia._
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.language.experimental.macros

trait SchemaFor[T] extends Serializable {
  self =>

  def schema(implicit info: AnnotationInfo = AnnotationInfo.Empty): Schema

  def write(t: T): Either[Record, AnyRef]
}

object SchemaFor extends SchemaForHelper {
  type Typeclass[T] = SchemaFor[T]

  def combine[T](caseClass: CaseClass[Typeclass, T])
                (implicit namingStrategy: NamingStrategy = DefaultNamingStrategy): Typeclass[T] = new Typeclass[T] {
    self =>
    override def schema(implicit info: AnnotationInfo): Schema = {
      if (caseClass.isObject || caseClass.isValueClass) {
        SchemaBuilder.enumeration(caseClass.typeName.short).namespace(caseClass.typeName.owner)
          .symbols(caseClass.typeName.short)
      } else {
        //sub field parser
        val fields = caseClass.parameters.map { p =>
          implicit val info = AnnotationInfo(p.annotations)
          newField(p.label, p.typeclass.schema, info, p.default)
        }.filter(_.schema() != null)

        val typeName = caseClass.typeName
        val info = AnnotationInfo(caseClass.annotations)
        val record = Schema.createRecord(
          namingStrategy.to(info.name.getOrElse(typeName.short)),
          info.doc.orNull,
          info.namespace.getOrElse(typeName.owner),
          false
        )
        record.setFields(fields.asJava)
        record
      }
    }

    override def write(t: T): Either[Record, AnyRef] = ???
  }

  def dispatch[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = new Typeclass[T] {
    self =>
    override def schema(implicit info: AnnotationInfo): Schema = {
      val schemas = sealedTrait.subtypes.map(_.typeclass.schema)
      val doc: String = null
      val typeName = sealedTrait.typeName
      if (schemas.forall(_.getType == Schema.Type.ENUM)) {
        val list = schemas.flatMap(_.getEnumSymbols.asScala)
        SchemaBuilder.enumeration(typeName.short).namespace(typeName.owner)
          .symbols(list: _*)
      } else {
        createSafeUnion(schemas: _*)
      }
    }

    override def write(t: T): Either[Record, AnyRef] = ???
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  def newField(name: String, schema: Schema, info: AnnotationInfo, default: Any)
              (implicit namingStrategy: NamingStrategy): Schema.Field = {
    val field = new Schema.Field(namingStrategy.to(info.name.getOrElse(name)),
      schema,
      info.doc.orNull,
      resolveDefault(default))
    info.aliases.foreach(field.addAlias)
    info.props.foreach(x => field.addProp(x._1, x._2))
    field
  }

  def overrideNamespace(schema: Schema, namespace: String): Schema = {
    schema.getType match {
      case Schema.Type.RECORD =>
        val fields = schema.getFields.asScala.map { field =>
          new Schema.Field(field.name(), overrideNamespace(field.schema(), namespace), field.doc, field.defaultVal, field.order)
        }
        val copy = Schema.createRecord(schema.getName, schema.getDoc, namespace, schema.isError, fields.asJava)
        schema.getAliases.asScala.foreach(copy.addAlias)
        schema.getObjectProps.asScala.foreach { case (k, v) => copy.addProp(k, v) }
        copy
      case Schema.Type.UNION => Schema.createUnion(schema.getTypes.asScala.map(overrideNamespace(_, namespace)).asJava)
      case Schema.Type.ENUM => Schema.createEnum(schema.getName, schema.getDoc, namespace, schema.getEnumSymbols)
      case Schema.Type.FIXED => Schema.createFixed(schema.getName, schema.getDoc, namespace, schema.getFixedSize)
      case Schema.Type.MAP => Schema.createMap(overrideNamespace(schema.getValueType, namespace))
      case Schema.Type.ARRAY => Schema.createArray(overrideNamespace(schema.getElementType, namespace))
      case _ => schema
    }
  }

  private def resolveDefault(default: Any): AnyRef = {
    default match {
      case null => null
      case None => null
      case uuid: UUID => uuid.toString
      case bd: BigDecimal => java.lang.Double.valueOf(bd.underlying.doubleValue)
      case Some(value) => resolveDefault(value)
      case b: Boolean => java.lang.Boolean.valueOf(b)
      case i: Int => java.lang.Integer.valueOf(i)
      case d: Double => java.lang.Double.valueOf(d)
      case s: Short => java.lang.Short.valueOf(s)
      case l: Long => java.lang.Long.valueOf(l)
      case f: Float => java.lang.Float.valueOf(f)
      case other => other.toString
    }
  }
}