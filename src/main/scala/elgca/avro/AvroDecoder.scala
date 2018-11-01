package elgca.avro

import java.nio.ByteBuffer

trait AvroDecoder[T] {
  self =>
  def decode(anyRef: AnyRef): T

  def map[S](fn: T => S): AvroDecoder[S] = new AvroDecoder[S] {
    override def decode(value: AnyRef): S = fn(self.decode(value))
  }
}

trait AvroDecoderImplicit {
  @inline private def const[T](f: AnyRef => T): AvroDecoder[T] = new AvroDecoder[T] {
    override def decode(anyRef: AnyRef): T = f(anyRef)
  }

  //primary types
  final implicit val StringDecoder: AvroDecoder[String] = const(_.asInstanceOf[String])
  final implicit val LongDecoder: AvroDecoder[Long] = const(_.asInstanceOf[java.lang.Number].longValue())
  final implicit val IntDecoder: AvroDecoder[Int] = const(_.asInstanceOf[java.lang.Number].intValue())
  final implicit val DoubleDecoder: AvroDecoder[Double] = const(_.asInstanceOf[java.lang.Number].doubleValue())
  final implicit val FloatDecoder: AvroDecoder[Float] = const(_.asInstanceOf[java.lang.Number].floatValue())
  final implicit val BooleanDecoder: AvroDecoder[Boolean] = const(_.asInstanceOf[java.lang.Boolean].booleanValue())
  final implicit val ByteBufferDecoder: AvroDecoder[ByteBuffer] = const(x => x.asInstanceOf[ByteBuffer])
  final implicit val arrayByteDecoder: AvroDecoder[Array[Byte]] = ByteBufferDecoder.map(x => x.array())
  final implicit val ShortDecoder: AvroDecoder[Short] = const(t => t.asInstanceOf[java.lang.Number].shortValue())
  final implicit val ByteDecoder: AvroDecoder[Byte] = const(t => t.asInstanceOf[java.lang.Number].byteValue())
}
