package elgca.avro.schema

import org.apache.avro.Schema

import scala.language.experimental.macros

trait AvroDecoder[T] {
  def decode(v: AnyRef): T
}

trait AvroSchema[T] {
  private[schema] def schema(info: MetaInfo): Schema
}
