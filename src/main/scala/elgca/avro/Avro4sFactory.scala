package elgca.avro

import magnolia._
import org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.experimental.macros


trait Avro4sFactory[T] {
  self =>
  protected var _index: Integer = _
  protected var _decimalMode: AvroDecimalMode = _
  protected var _name: String = _
  protected var _namespace: String = _
  protected var _doc: String = _
  protected val _aliases: mutable.ListBuffer[String] = new mutable.ListBuffer[String]
  protected val _props: mutable.Map[String, String] = new mutable.HashMap[String, String]()
  protected var _default: Option[T] = None
  protected var _schema: Schema = _

  protected def isObject: Boolean = false

  protected def setIndex(index: Int): Avro4sFactory[T] = {
    _index = index
    self
  }

  protected def setDefault(t: T): Avro4sFactory[T] = {
    _default = Option(t)
    self
  }

  protected def setDefault(t: Option[T]): Avro4sFactory[T] = {
    _default = t
    self
  }

  protected def setDecimalMode(decimalMode: AvroDecimalMode): Avro4sFactory[T] = {
    if (decimalMode != null) self._decimalMode = decimalMode
    self
  }

  protected def setName(name: String): Avro4sFactory[T] = {
    self._name = name
    self
  }

  protected def getName(implicit naming: NamingStrategy): String = {
    naming.to(self._name)
  }

  protected def setNamespace(namespace: String): Avro4sFactory[T] = {
    self._namespace = namespace
    self
  }

  protected def setDoc(doc: String): String = {
    self._doc = doc
    doc
  }

  protected def setAliases(aliases: Seq[String]): Avro4sFactory[T] = {
    self._aliases ++= aliases
    self
  }

  protected def setAlias(alias: String): Avro4sFactory[T] = {
    self._aliases += alias
    self
  }

  protected def setProps(props: Map[String, String]): Avro4sFactory[T] = {
    self._props ++= props
    self
  }

  protected def setProps(k: String, v: String): Avro4sFactory[T] = {
    self._props += ((k, v))
    self
  }

  protected def fromAnnotations(annos: Seq[AvroAnnotation]): Avro4sFactory[T] = {
    annos.foreach {
      case AvroNamespace(namespace) => setNamespace(namespace)
      case AvroAlias(alias) => setAlias(alias)
      case AvroDoc(doc) => setDoc(doc)
      case AvroName(name) => setName(name)
      case AvroProp(k, v) => setProps(k, v)
      case x: AvroDecimalMode => setDecimalMode(x)
    }
    self
  }

  protected def fromAnnos(annos: Seq[Any]): Avro4sFactory[T] = {
    fromAnnotations(annos.filter(_.isInstanceOf[AvroAnnotation]).asInstanceOf[Seq[AvroAnnotation]])
  }

  protected def fromFactory(other: Avro4sFactory[_]) = {
    _decimalMode = other._decimalMode
    _namespace = other._namespace
    //    namingStrategy = other.namingStrategy
    self
  }

  def build(): Avro4s[T]

  private var avro4s: Avro4s[T] = null

  def getOrCreate(): Avro4s[T] = {
    if (avro4s == null) {
      avro4s = build()
    }
    avro4s
  }

  def field(schema: Schema)(implicit namingStrategy: NamingStrategy): Schema.Field = {
    val field = new Schema.Field(getName,
      schema,
      _doc,
      null /*resolveDefault(_default)*/)
    _aliases.foreach(field.addAlias)
    _props.foreach(x => field.addProp(x._1, x._2))
    field
  }

  private def resolveDefault(default: Any): AnyRef = {
    default match {
      case null => null
      case None => null
      case Some(value) => resolveDefault(value)
      case bd: BigDecimal => java.lang.Double.valueOf(bd.underlying.doubleValue)
      case b: Boolean => java.lang.Boolean.valueOf(b)
      case i: Int => java.lang.Integer.valueOf(i)
      case d: Double => java.lang.Double.valueOf(d)
      case s: Short => java.lang.Short.valueOf(s)
      case l: Long => java.lang.Long.valueOf(l)
      case f: Float => java.lang.Float.valueOf(f)
      case _ => throw new IllegalArgumentException("unsupported default value")
    }
  }
}

object Avro4sFactory {
  type Typeclass[T] = Avro4sFactory[T]

  def combine[T](caseClass: CaseClass[Typeclass, T])
                (implicit naming: NamingStrategy = DefaultNamingStrategy): Typeclass[T] = {
    val typeName = caseClass.typeName
    if (caseClass.isObject) {
      new Typeclass[T] {
        setName(typeName.short)
        setNamespace(typeName.owner)
        fromAnnos(caseClass.annotations)

        override protected def isObject: Boolean = true


        override def build(): Avro4s[T] = {
          new Avro4s[T] {
            override def schema: Schema = throw new UnsupportedOperationException

            override def decode(anyRef: AnyRef): T = caseClass.rawConstruct(Nil)

            override def encode(t: T): AnyRef = throw new UnsupportedOperationException
          }
        }
      }
    } else {
      new Typeclass[T] {
        self =>
        setName(typeName.short)
        setNamespace(typeName.owner)

        override def build(): Avro4s[T] = {
          fromAnnos(caseClass.annotations)
          caseClass.parameters.foreach { p =>
            p.typeclass
              .fromFactory(self)
              .setIndex(p.index)
              .setName(p.label)
              .setDefault(p.default)
              .fromAnnos(p.annotations)
          }

          val fields = caseClass.parameters.map(x => x.typeclass.field(x.typeclass.getOrCreate().schema))
          val record = Schema.createRecord(
            self.getName,
            self._doc,
            self._namespace,
            false
          )
          record.setFields(fields.asJava)

          val forIndex = caseClass.parameters.map(x => x.typeclass.getName -> x.typeclass._index).toMap
          new Avro4s[T] {
            avro4s =>
            private var _schema: Schema = _

            override def schema: Schema = {
              if (avro4s._schema == null) {
                avro4s._schema = record
              }
              avro4s._schema
            }

            override def decode(anyRef: AnyRef): T = {
              anyRef match {
                case record: IndexedRecord =>
                  caseClass.construct(x => {
                    val value = record.get(x.index)
                    val r = x.typeclass.getOrCreate().decode(value)
                    //                    if (r == null) x.default.orNull else r
                    r
                  })
                case _ => throw new IllegalArgumentException("不是Record对象")
              }
            }

            override def encode(t: T): AnyRef = {
              val data = caseClass.parameters.map {
                x =>
                  x.typeclass.getOrCreate().encode(x.dereference(t))
              }.toArray
              new ImmutableRecord {
                override def get(key: String): AnyRef = get(forIndex(key))

                override def get(i: Int): AnyRef = data(i)

                override def getSchema: Schema = schema
              }
            }
          }
        }
      }
    }
  }

  def dispatch[T](sealedTrait: SealedTrait[Typeclass, T])
                 (implicit naming: NamingStrategy = DefaultNamingStrategy): Typeclass[T] = {
    val subtypes = sealedTrait.subtypes
    val typeName = sealedTrait.typeName
    val (enum, records) = subtypes.partition(_.typeclass.isObject)
    val list = enum.map(_.typeName.short)
    val enumSchema = list.headOption.map(x => {
      SchemaBuilder
        .enumeration(typeName.short)
        .namespace(typeName.owner)
        .symbols(list: _*)
    })

    new Typeclass[T] {
      self =>
      setName(typeName.short)
      setNamespace(typeName.owner)

      override def build(): Avro4s[T] = {
        fromAnnos(sealedTrait.annotations)
        subtypes.foreach(x => x.typeclass.fromFactory(self))
        val sch =
          if (records.isEmpty) {
            enumSchema.get
          } else if (enumSchema.isEmpty) {
            createSafeUnion(records.map(_.typeclass.getOrCreate().schema): _*)
          } else {
            createSafeUnion(enumSchema.get +: records.map(_.typeclass.getOrCreate().schema): _*)
          }
        new Avro4s[T] {
          avro4s =>
          private val subAvro = records
          private var _schema: Schema = _

          override def schema: Schema = {
            if (_schema == null) {
              _schema = sch
            }
            _schema
          }

          override def decode(anyRef: AnyRef): T = {
            anyRef match {
              case enum: EnumSymbol =>
                val x = sealedTrait.subtypes.find(_.typeName.short == enum.toString).get
                x.typeclass.getOrCreate().decode(anyRef)
              case record: IndexedRecord =>
                val x = sealedTrait.subtypes.find(_.typeclass.getName == record.getSchema.getName).get
                x.typeclass.getOrCreate().decode(anyRef)
            }
          }

          override def encode(t: T): AnyRef = {
            sealedTrait.dispatch(t) {
              x =>
                x.typeclass.isObject match {
                  case true => new EnumSymbol(schema, x.typeName.short)
                  case false => x.typeclass.getOrCreate().encode(x.cast(t))
                }
            }
          }
        }
      }
    }
  }

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  def createSafeUnion(schemas: Schema*): Schema = {
    val flattened = schemas.flatMap {
      x =>
        if (x.getType == Schema.Type.UNION)
          x.getTypes.asScala
        else
          Seq(x)
    }
    val (nulls, rest) = flattened.partition(_.getType == Schema.Type.NULL)
    Schema.createUnion(nulls.headOption.toSeq ++ rest: _*)
  }
}
