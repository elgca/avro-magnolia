package elgca.avro

import java.nio.ByteBuffer
import java.util.UUID

import scala.language.higherKinds

trait AvroEncoder[T] {
  self =>
  def encode(t: T): AnyRef

  def map[S](fn: S => T): AvroEncoder[S] = new AvroEncoder[S] {
    override def encode(value: S): AnyRef = self.encode(fn(value))
  }
}

trait AvroEncoderImplicit {

  @inline private def const[T](f: T => AnyRef): AvroEncoder[T] = new AvroEncoder[T] {
    def encode(t: T): AnyRef = f(t)
  }

  //primary types
  final implicit val StringEncoder: AvroEncoder[String] = const(identity[String])
  final implicit val LongEncoder: AvroEncoder[Long] = const(java.lang.Long.valueOf)
  final implicit val IntEncoder: AvroEncoder[Int] = const(java.lang.Integer.valueOf)
  final implicit val DoubleEncoder: AvroEncoder[Double] = const(java.lang.Double.valueOf)
  final implicit val FloatEncoder: AvroEncoder[Float] = const(java.lang.Float.valueOf)
  final implicit val BooleanEncoder: AvroEncoder[Boolean] = const(java.lang.Boolean.valueOf)
  final implicit val ByteBufferEncoder: AvroEncoder[ByteBuffer] = const(identity[ByteBuffer])
  final implicit val arrayByteEncoder: AvroEncoder[Array[Byte]] = ByteBufferEncoder.map(ByteBuffer.wrap)
  final implicit val ShortEncoder: AvroEncoder[Short] = const(t => java.lang.Integer.valueOf(t))
  final implicit val ByteEncoder: AvroEncoder[Byte] = const(t => java.lang.Integer.valueOf(t))
  final implicit val UUIDEncoder: AvroEncoder[UUID] = StringEncoder.map[UUID](_.toString)

  //  implicit val decimalEncoder: AvroEncoder[BigDecimal] = StringEncoder.comap[BigDecimal](_.toString)
}