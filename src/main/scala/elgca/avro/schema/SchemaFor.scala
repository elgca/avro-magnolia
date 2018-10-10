package org.elgca.avro.schema

import org.apache.avro.{Schema, SchemaBuilder}
import magnolia._

import scala.language.experimental.macros
import java.util.{UUID, List => JList, Map => JMap}

import org.elgca.avro.Util.schema

import scala.collection.JavaConverters._

trait SchemaFor[T] extends Serializable {
  self =>

  def schema: Schema

  def map[U](fn: Schema => Schema): SchemaFor[U] = new SchemaFor[U] {
    override def schema: Schema = fn(self.schema)
  }
}

object SchemaFor {
  type Typeclass[T] = SchemaFor[T]

  def combine[T](caseClass: CaseClass[Typeclass, T]): Typeclass[T] = new Typeclass[T] {
    override def schema: Schema = {
      if (caseClass.isObject || caseClass.isValueClass) {
        SchemaBuilder.enumeration(caseClass.typeName.short).namespace(caseClass.typeName.owner)
          .symbols(caseClass.typeName.short)
      } else {
        val typeName = caseClass.typeName
        val fields = caseClass.parameters.map { p =>
          newField(p.label, p.typeclass.schema, null, p.default.orNull)
        }.filter(_.schema() != null)
        val record = Schema.createRecord(typeName.short, null,
          typeName.owner, false)
        record.setFields(fields.asJava)
        record
      }
    }
  }

  def dispatch[T](sealedTrait: SealedTrait[Typeclass, T]): Typeclass[T] = new Typeclass[T] {
    override def schema: Schema = {
      val schemas = sealedTrait.subtypes.map(_.typeclass.schema)
      val doc: String = null
      val typeName = sealedTrait.typeName
      if (schemas.forall(_.getType == Schema.Type.ENUM)) {
        val list = schemas.flatMap(_.getEnumSymbols.asScala)
        SchemaBuilder.enumeration(typeName.short).namespace(typeName.owner)
          .symbols(list: _*)
      } else throw new RuntimeException("not support yet")
    }
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  def newField(name: String, schema: Schema, doc: String, default: Any): Schema.Field = {
    new Schema.Field(DefaultNamingStrategy.to(name),
      schema,
      doc,
      resolveDefault(default))
  }

  private def resolveDefault(default: Any): AnyRef = {
    default match {
      case null => null
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