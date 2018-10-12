package elgca.avro.schema

import java.util.UUID

import magnolia._
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.language.experimental.macros

case class AvroDerivation[T](schema: MetaInfo => Schema, encode: (T, Schema) => AnyRef)

object Avro4s extends AvroSchemaHelper {
  type Typeclass[T] = AvroDerivation[T]

  def combine[T](caseClass: CaseClass[Typeclass, T])
                (implicit namingStrategy: NamingStrategy = DefaultNamingStrategy): Typeclass[T] = {
    def schema(metaInfo: MetaInfo): Schema = {
      implicit val base: MetaInfo = metaInfo
      if (caseClass.isObject || caseClass.isValueClass) {
        SchemaBuilder
          .enumeration(caseClass.typeName.short)
          .namespace(caseClass.typeName.owner)
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

    def decode(t: T, schema: Schema): AnyRef = {
      if (caseClass.isObject || caseClass.isValueClass) {
        new EnumSymbol(schema, caseClass.typeName.short)
      } else {
        val fields = caseClass.parameters.map { p =>
          val info: MetaInfo = MetaInfo(p.annotations)(MetaInfo.Empty)
          val name = getName(p.label, info)
          val any = p.typeclass.encode(p.dereference(t))
          (p.index, name, any)
        }
        new ImmutableRecord {
          private val _data = fields
          private val _schema = schema

          override def get(key: String): AnyRef = _data.find(_._2 == key).get

          override def get(i: Int): AnyRef = _data.find(_._1 == i).get

          override def getSchema: Schema = _schema
        }
      }
    }

    AvroDerivation(schema, decode)
  }

  def dispatch[T](sealedTrait: SealedTrait[Typeclass, T])
                 (implicit namingStrategy: NamingStrategy = DefaultNamingStrategy): Typeclass[T] = {
    def schema(metaInfo: MetaInfo): Schema = {
      implicit val base: MetaInfo = metaInfo
      val info = MetaInfo(sealedTrait.annotations)
      val schemas = sealedTrait.subtypes.map(_.typeclass.schema(info))
      val typeName = sealedTrait.typeName
      val (enum, records) = schemas.partition(_.getType == Schema.Type.ENUM)
      val list = enum.flatMap(_.getEnumSymbols.asScala)
      val enumSchema = SchemaBuilder
        .enumeration(typeName.short)
        .namespace(typeName.owner)
        .symbols(list: _*)
      val ss = records :+ enumSchema
      if (ss.size > 1)
        createSafeUnion(ss: _*)
      else
        ss.head
    }

    def decode(t: T, schema: Schema): AnyRef = {
      sealedTrait.dispatch(t) {
        p =>
          p.typeclass.encode(p.cast(t), schema)
      }
    }

    AvroDerivation(schema, decode)
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