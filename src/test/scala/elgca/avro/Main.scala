package elgca.avro

import scala.collection.{immutable, mutable}
sealed trait BLC

case object BitCon extends BLC

case object LatCon extends BLC

case class DeciTest(dcm:BigDecimal,either: Either[Long,String])

case class Test2(opt: String,
                 int: Int,
                 uuid: immutable.Map[String, String],
                 array: List[Byte],
                 seq: List[String],
                 opTion: Option[List[String]],
                 @AvroDecimalMode(20,10)
                 dcs:BigDecimal,
//                 @AvroDecimalMode(38,10)
                 a:DeciTest)

case class TBC(id: String, value: Test2, enu: BLC)
object Main extends App {

  val map = (1 to 9).map(x => x -> x).toMap
//  implicit val naming = SnakeCase
//  implicit val decimalMode = AvroDecimalMode(22, 5)
  val avro4s = Avro4sFactory.gen[TBC].build()
  val hash: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
  hash.put("1", "2")
  val b: mutable.HashMap[String, String] = hash.map(x => x)

  println(avro4s.schema)
  val dcs = DeciTest(BigDecimal("458.12544444444444444444444"),Right("66868"))
  val clz = Test2("666", 625, Map.empty, "nihao".getBytes.toList, "A" :: "B" :: Nil, Some("A" :: "B" :: Nil),BigDecimal("3.14"),dcs)
  val tbc = TBC("666", clz, LatCon)
  println(tbc)
  val avroRow = avro4s.encode(tbc)
  println(avroRow)
  println(avro4s.decode(avroRow))
}
