package elgca.avro.schema

import java.sql.Date

case class Ingredient(name: Human, sugar: String, fat: List[Byte])
case class Pizza(name: (Int,Double),
                 ingredients: Vector[Ingredient],
                 vegetarian: Either[Ingredient,String],
                 vegan:  Map[String,String],
                 binary: List[Byte],
                 opt: String,
                 calories: Int)
//name: String,
//opt: Human,
//binary: List[Byte],
//ingredients: Vector[Sub],
//vegetarian: Either[Sub,String],
//vegan: Date,
//colorEnum: ColorEnum,
//@AvroDecimalMode(38,10)
//calories: BigDecimal
object Tes extends App {
  import com.sksamuel.avro4s.AvroSchema
  val schema3 = AvroSchema[Pizza]
  println(schema3)
}
