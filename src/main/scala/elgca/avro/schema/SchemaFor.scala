package elgca.avro.schema

import java.util.{UUID, List => JList, Map => JMap}

import magnolia._
import org.apache.avro.{Schema, SchemaBuilder}
import reactivemongo.bson.BSONHandler
import org.apache.avro.generic.GenericData.EnumSymbol
import scala.collection.JavaConverters._
import scala.language.experimental.macros

trait AvroEncoder[T] {
  def encode(t: T): Either[Record, AnyRef]
}

trait AvroDecoder[T] {
  def decode(v: Either[Record, AnyRef]): T
}

object AType {
  type AvroValue = Either[Record, AnyRef]
}

import AType._

trait SchemaFor[T] extends Serializable {

  //  def schema: Schema = schema(MetaInfo.Empty)
  def build: Schema = schema(MetaInfo.Empty)

  private[schema] def schema(implicit info: MetaInfo): Schema

  def write(t: T)(implicit metaInfo: MetaInfo): Schema => AvroValue = ???

  def read(v: AvroValue): T = ???

}

object SchemaFor extends SchemaForHelper {
  type Typeclass[T] = SchemaFor[T]

  def combine[T](caseClass: CaseClass[Typeclass, T])
                (implicit namingStrategy: NamingStrategy = DefaultNamingStrategy): Typeclass[T] = {
    new Typeclass[T] {
      override def schema(implicit metaInfo: MetaInfo): Schema = {
        if (caseClass.isObject || caseClass.isValueClass) {
          SchemaBuilder.enumeration(caseClass.typeName.short).namespace(caseClass.typeName.owner)
            .symbols(caseClass.typeName.short)
        } else {
          //sub field parser
          val fields = caseClass.parameters.map { p =>
            val info: MetaInfo = MetaInfo(p.annotations)
            val name = getName(p.label, info)
            newField(name, p.typeclass.schema(info), info, p.default)
          }

          val typeName = caseClass.typeName
          val info = MetaInfo(caseClass.annotations)
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

      override def write(t: T)(implicit metaInfo: MetaInfo): Schema => AvroValue = {
        sch =>
          val out: Seq[(Int, String, AvroValue)] = caseClass.parameters.map { p =>
            val info: MetaInfo = MetaInfo(p.annotations)
            val name = getName(p.label, info)
            val subSchema = p.typeclass.schema(info)
            val value = p.dereference(t)
            (p.index, name, p.typeclass.write(value)(info)(subSchema))
          }
          Left(
            new ImmutableRecord {
              override def get(key: String): AnyRef = out.find(_._2 == key).get

              override def get(i: Int): AnyRef = out(i)

              override def getSchema: Schema = sch
            }
          )
      }
    }
  }

  def dispatch[T](sealedTrait: SealedTrait[Typeclass, T])
                 (implicit namingStrategy: NamingStrategy = DefaultNamingStrategy): Typeclass[T] = {
    new Typeclass[T] {
      override def write(t: T)(implicit metaInfo: MetaInfo): Schema => AvroValue = super.write(t)

      override def schema(implicit metaInfo: MetaInfo): Schema = {
        val info = MetaInfo(sealedTrait.annotations)
        val schemas = sealedTrait.subtypes.map(_.typeclass.schema(info))
        val typeName = sealedTrait.typeName
        if (schemas.forall(_.getType == Schema.Type.ENUM)) {
          val list = schemas.flatMap(_.getEnumSymbols.asScala)
          SchemaBuilder.enumeration(typeName.short).namespace(typeName.owner)
            .symbols(list: _*)
        } else {
          createSafeUnion(schemas: _*)
        }
      }
    }
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  def newField(name: String, schema: Schema, info: MetaInfo, default: Any): Schema.Field = {
    val field = new Schema.Field(name,
      schema,
      info.doc.orNull,
      resolveDefault(default))
    info.aliases.foreach(field.addAlias)
    info.props.foreach(x => field.addProp(x._1, x._2))
    field
  }

  def getName(name: String, info: MetaInfo)(implicit namingStrategy: NamingStrategy): String = {
    namingStrategy.to(info.name.getOrElse(name))
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