package org.elgca.avro.schema

import java.nio.ByteBuffer

import org.apache.avro.{Schema, SchemaBuilder}

object SchemaForImp {
  def const[T](_schema: Schema) = new SchemaFor[T] {
    override def schema: Schema = _schema
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
  implicit val ShortSchemaFor: SchemaFor[Short] = const(IntSchemaFor.schema)
  implicit val ByteSchemaFor: SchemaFor[Byte] = const(IntSchemaFor.schema)

  implicit def seqSchemaFor[T](implicit schemaFor: SchemaFor[T]):SchemaFor[Seq[T]] = {
    new SchemaFor[Seq[T]] {
      override def schema: Schema = Schema.createArray(schemaFor.schema)
    }
  }

  implicit def setSchemaFor[T](implicit schemaFor: SchemaFor[T]):SchemaFor[Set[T]] = {
    new SchemaFor[Set[T]] {
      override def schema: Schema = Schema.createArray(schemaFor.schema)
    }
  }

  implicit def vectorSchemaFor[S](implicit schemaFor: SchemaFor[S]): SchemaFor[Vector[S]] = {
    new SchemaFor[Vector[S]] {
      override def schema: Schema = Schema.createArray(schemaFor.schema)
    }
  }

}
