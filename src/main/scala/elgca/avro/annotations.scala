package elgca.avro

import scala.annotation.StaticAnnotation
import scala.math.BigDecimal.RoundingMode.{RoundingMode, _}

sealed trait AvroAnnotation extends StaticAnnotation

case class AvroAlias(alias: String) extends AvroAnnotation
case class AvroDoc(doc: String) extends AvroAnnotation

/**
  * 在avro中使用别名
  *
  * @param name
  */
case class AvroName(name: String) extends AvroAnnotation

/**
  * 修改package名称
  *
  * @param namespace
  */
case class AvroNamespace(namespace: String) extends AvroAnnotation

/**
  * 附加属性
  *
  * @param name
  * @param value
  */
case class AvroProp(name: String, value: String) extends AvroAnnotation

/**
  * 用于设置BigDecimal类型精度
  *
  * @param precision
  * @param scale
  * @param roundingMode
  */
case class AvroDecimalMode(precision: Int, scale: Int, roundingMode: RoundingMode = HALF_UP) extends AvroAnnotation

trait AvroDecimalModeImplicit {
  implicit val default = AvroDecimalMode(8, 2)
}