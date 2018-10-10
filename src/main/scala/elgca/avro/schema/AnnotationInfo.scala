package elgca.avro.schema

class AnnotationInfo(val annos: Seq[AvroAnnotation]) {
  lazy val namespace: Option[String] = {
    annos.find(_.isInstanceOf[AvroNamespace])
      .map(_.asInstanceOf[AvroNamespace].namespace)
  }

  lazy val doc: Option[String] = {
    annos.find(_.isInstanceOf[AvroDoc])
      .map(_.asInstanceOf[AvroDoc].doc)
  }

  lazy val aliases: Seq[String] = {
    annos.filter(_.isInstanceOf[AvroAlias])
      .map(_.asInstanceOf[AvroAlias].alias)
  }

  lazy val name: Option[String] = {
    annos.find(_.isInstanceOf[AvroName])
      .map(_.asInstanceOf[AvroName].name)
  }

  lazy val props: Map[String, String] = {
    annos.find(_.isInstanceOf[AvroProp])
      .map { x =>
        val a = x.asInstanceOf[AvroProp]
        (a.name, a.value)
      }.toMap
  }

  lazy val decimalMode: Option[AvroDecimalMode] = {
    annos.find(_.isInstanceOf[AvroDecimalMode]).map(_.asInstanceOf[AvroDecimalMode])
  }

  override def toString: String = s"AnnotationInfo{%s}" format {
    if (annos.isEmpty) "" else annos.map(_.toString).mkString("\n", "\n", "\n")
  }
}

object AnnotationInfo {
  def apply(annos: Seq[Any])(implicit default: AvroDecimalMode = AvroDecimalMode.default): AnnotationInfo = {
    new AnnotationInfo((annos :+ default).filter(_.isInstanceOf[AvroAnnotation]).map(_.asInstanceOf[AvroAnnotation]))
  }

  def Empty(implicit default: AvroDecimalMode = AvroDecimalMode.default) = apply(Nil)
}