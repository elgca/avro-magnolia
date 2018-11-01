package elgca.avro

import org.apache.avro.Schema

import scala.reflect.runtime.universe._

abstract class Avro4s[T] {
  def schema: Schema

  def decode(anyRef: AnyRef): T

  def encode(t: T): AnyRef
}

object Avro4s {
  def build[T: WeakTypeTag](implicit factory: Avro4sFactory[T]): Avro4s[T] = factory.build()
}