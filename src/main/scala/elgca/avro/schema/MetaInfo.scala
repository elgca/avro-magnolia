package elgca.avro.schema

class MetaInfo(val annos: Seq[AvroAnnotation]) {
  def map(f: PartialFunction[AvroAnnotation, AvroAnnotation]): MetaInfo = {
    if (annos.isEmpty || annos.forall(f.isDefinedAt)) this
    else new MetaInfo(annos.map { x => f.applyOrElse(x, identity[AvroAnnotation]) })
  }

  def append(anno: AvroAnnotation*): MetaInfo = {
    new MetaInfo(annos ++ anno)
  }

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

object MetaInfo {
  def apply(annos: Seq[Any])(implicit default: MetaInfo): MetaInfo = {
      val ann = annos.filter(_.isInstanceOf[AvroAnnotation]).map(_.asInstanceOf[AvroAnnotation])
      new MetaInfo(ann ++ default.decimalMode)
  }

  def Empty = new MetaInfo(Nil)
}