package elgca.avro.schema

import java.nio.ByteBuffer

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.language.higherKinds
import scala.reflect.ClassTag

trait AvroEncoder[T] {
  self =>
  def encode(t: T): AnyRef

  def comap[S](fn: S => T): AvroEncoder[S] = new AvroEncoder[S] {
    override def encode(value: S): AnyRef = self.encode(fn(value))
  }
}

object AvroEncoderHelper {

  def const[T](f: T => AnyRef): AvroEncoder[T] = new AvroEncoder[T] {
    def encode(t: T): AnyRef = f(t)
  }

  //primary types
  implicit val StringEncoder: AvroEncoder[String] = const(identity[String])
  implicit val LongEncoder: AvroEncoder[Long] = const(java.lang.Long.valueOf)
  implicit val IntEncoder: AvroEncoder[Int] = const(java.lang.Integer.valueOf)
  implicit val DoubleEncoder: AvroEncoder[Double] = const(java.lang.Double.valueOf)
  implicit val FloatEncoder: AvroEncoder[Float] = const(java.lang.Float.valueOf)
  implicit val BooleanEncoder: AvroEncoder[Boolean] = const(java.lang.Boolean.valueOf)
  implicit val ByteBufferEncoder: AvroEncoder[ByteBuffer] = const(identity[ByteBuffer])
  implicit val ShortEncoder: AvroEncoder[Short] = const(t => java.lang.Integer.valueOf(t))
  implicit val ByteEncoder: AvroEncoder[Byte] = const(t => java.lang.Integer.valueOf(t))

  implicit val UUIDEncoder: AvroEncoder[UUID] = StringEncoder.comap[UUID](_.toString)

}