package fauna.sbt

object CodexHelpers {
  private def mkList(arity: Int)(f: Int => String) = 1 to arity map f

  private def mkTypes(ar: Int) = mkList(ar) { "T" + _ } mkString ", "
  private def mkKeyParams(ar: Int) = mkList(ar) {
    "k" + _ + ": String"
  } mkString ", "
  private def mkDecoderParams(ar: Int) = mkList(ar) { i =>
    "dec" + i + ": base.Decoder[T" + i + "]"
  } mkString ", "
  private def mkFieldDecoderParams(ar: Int) = mkList(ar) { i =>
    "dec" + i + ": base.FieldDecoder[T" + i + "]"
  } mkString ", "
  private def mkEncoderParams(ar: Int) = mkList(ar) { i =>
    "enc" + i + ": base.Encoder[T" + i + "]"
  } mkString ", "
  private def mkFieldEncoderParams(ar: Int) = mkList(ar) { i =>
    "enc" + i + ": base.FieldEncoder[T" + i + "]"
  } mkString ", "
  private def mkKeys(ar: Int) = mkList(ar) { "k" + _ } mkString ", "
  private def mkDecoders(ar: Int) = mkList(ar) { "dec" + _ } mkString ", "
  private def mkEncoders(ar: Int) = mkList(ar) { "enc" + _ } mkString ", "
  private def mkInnerDecoderMap(ar: Int) = "Map(" + (mkList(ar) { i =>
    "k" + i + " -> dec" + i + ".innerDecoder"
  } mkString ", ") + ")"
  private def mkInnerEncoderMap(ar: Int) = "Map(" + (mkList(ar) { i =>
    "k" + i + " -> enc" + i + ".innerEncoder"
  } mkString ", ") + ")"
  private def mkInnerDecoderVec(ar: Int) =
    "Vector(" + (mkList(ar) { "dec" + _ } mkString ", ") + ")"
  private def mkInnerEncoderSeq(ar: Int) =
    "Seq(" + (mkList(ar) { "enc" + _ } mkString ", ") + ")"

  private def mkRecordDecodeDef(ar: Int) = {
    val varinits = mkList(ar) { i =>
      "var v" + i + " = null.asInstanceOf[T" + i + "]"
    } mkString "\n"
    val decodes = mkList(ar) { i =>
      "if (key == k" + i + ") { v" + i + " = dec" + i + ".decode(valueStream) }"
    } mkString " else "
    val varchecks = mkList(ar) { i =>
      "dec" + i + ".check(k" + i + ", v" + i + ")"
    } mkString ", "

    ("""|  def decode(stream: In) = {
        |    """ + varinits + """
        |
        |    base.format.readRecord(stream, """ + ar + """) { (key, valueStream) =>
        |      """ + decodes + """ else { base.format.skipField(valueStream) }
        |    }
        |
        |    construct(""" + varchecks + """)
        |  }""").stripMargin
  }

  private def mkRecordEncodeDef(ar: Int) = {
    val vals = mkList(ar) { "v" + _ } mkString ", "
    val sizeCalcs = mkList(ar) { i =>
      "if (base.format.shouldEncode(v" + i + ")) size += 1"
    } mkString "\n"
    val encodes = mkList(ar) { i =>
      "if (base.format.shouldEncode(v" + i + ")) { base.format.writeField(record, k" + i + ") { s => enc" + i + ".encodeTo(s, v" + i + ") } }"
    } mkString "\n"

    ("""|  def encodeTo(stream: Out, t: T): Unit = {
        |    val (""" + vals + """) = destruct(t).get
        |
        |    var size = 0
        |    """ + sizeCalcs + """
        |
        |    base.format.writeRecord(stream, size) { record =>
        |      """ + encodes + """
        |    }
        |  }""").stripMargin
  }

  private def mkTupleDecodeDef(ar: Int) = {
    val varinits = mkList(ar) { i =>
      "var v" + i + " = null.asInstanceOf[T" + i + "]"
    } mkString "\n"
    val decodes = mkList(ar) { i =>
      "case " + i + " => v" + i + " = dec" + i + ".decode(valueStream)"
    } mkString "\n"
    val vars = mkList(ar) { i => "v" + i } mkString ", "

    ("""|  def decode(stream: In) = {
        |    """ + varinits + """
        |    base.format.readTuple(stream, """ + ar + """) { (idx, valueStream) =>
        |      (idx + 1) match {
        |        """ + decodes + """
        |        case _ => base.format.skipField(valueStream)
        |      }
        |    }
        |
        |    (""" + vars + """)
        |  }""").stripMargin
  }

  private def mkTupleEncodeDef(ar: Int) = {
    // in encode
    val types = mkTypes(ar)
    val encodes = mkList(ar) { i =>
      "enc" + i + ".encodeTo(tupleStream, t._" + i + ")"
    } mkString "\n"

    ("""|  def encodeTo(stream: Out, t: (""" + types + """)): Unit = {
        |    base.format.writeTuple(stream, """ + ar + """) { tupleStream =>
        |      """ + encodes + """
        |    }
        |  }""").stripMargin
  }

  def genRecordDecoders = {
    def mkClass(arity: Int) = {

      // in definition
      val types = mkTypes(arity)
      val keyParams = mkKeyParams(arity)
      val decoderParams = mkFieldDecoderParams(arity)
      val keys = mkKeys(arity)
      val innerDecoderMap = mkInnerDecoderMap(arity)

      ("""|def Record[T, """ + types + """](""" + keyParams + """) =
          |  new RecordDecoderAssoc""" + arity + """(""" + keys + """)
          |
          |class RecordDecoderAssoc""" + arity + """(""" + keyParams + """) {
          |  def apply[T, """ + types + """](construct: (""" + types + """) => T)
          |                             (implicit """ + decoderParams + """) = {
          |    bindTo(construct)
          |  }
          |
          |  def bindTo[T, """ + types + """](construct: (""" + types + """) => T)
          |                            (implicit """ + decoderParams + """) = {
          |
          |    new RecordDecoder""" + arity + """[T, """ + types + """](
          |      construct,
          |      """ + keys + """,
          |      """ + mkDecoders(arity) + """
          |    )
          |  }
          |}
          |
          |class RecordDecoder""" + arity + """[T, """ + types + """](
          |    construct: (""" + types + """) => T,
          |    """ + keyParams + """,
          |    """ + decoderParams + """) extends base.RecordDecoder[T] {
          |
          |  def keys = Set(""" + keys + """)
          |  def innerDecoders = """ + innerDecoderMap + """
          |
          |  """ + mkRecordDecodeDef(arity) + """
          |}
          |""").stripMargin
    }

    val classes = 1 to 22 map mkClass mkString "\n\n"

    ("""|package fauna.codex.builder
        |
        |trait RecordDecoderConstructors[In] { self: RecordDecoderBuilder[In] =>
        |""" + classes + """
        |}
        |""").stripMargin
  }

  def genRecordEncoders = {
    def mkClass(arity: Int) = {

      // in definition
      val types = mkTypes(arity)
      val keyParams = mkKeyParams(arity)
      val encoderParams = mkFieldEncoderParams(arity)
      val keys = mkKeys(arity)
      val innerEncoderMap = mkInnerEncoderMap(arity)

      ("""|def Record[T, """ + types + """](""" + keyParams + """) =
          |  new RecordEncoderAssoc""" + arity + """(""" + keys + """)
          |
          |class RecordEncoderAssoc""" + arity + """(""" + keyParams + """) {
          |  def apply[T, """ + types + """](destruct: T => Option[(""" + types + """)])
          |                            (implicit """ + encoderParams + """) = {
          |    bindTo(destruct)
          |  }
          |
          |  def bindTo[T, """ + types + """](destruct: T => Option[(""" + types + """)])
          |                            (implicit """ + encoderParams + """) = {
          |
          |    new RecordEncoder""" + arity + """[T, """ + types + """](
          |      destruct,
          |      """ + keys + """,
          |      """ + mkEncoders(arity) + """
          |    )
          |  }
          |}
          |
          |class RecordEncoder""" + arity + """[T, """ + types + """](
          |    destruct: T => Option[(""" + types + """)],
          |    """ + keyParams + """,
          |    """ + encoderParams + """) extends base.RecordEncoder[T] {
          |
          |  def keys = Set(""" + keys + """)
          |  def innerEncoders = """ + innerEncoderMap + """
          |
          |  """ + mkRecordEncodeDef(arity) + """
          |}
          |""").stripMargin
    }

    val classes = 1 to 22 map mkClass mkString "\n\n"

    ("""|package fauna.codex.builder
        |
        |import language.existentials
        |
        |trait RecordEncoderConstructors[Out] { self: RecordEncoderBuilder[Out] =>
        |""" + classes + """
        |}
        |""").stripMargin
  }

  def genRecordCodecs = {
    def mkClass(arity: Int) = {

      // in definition
      val types = mkTypes(arity)
      val keyParams = mkKeyParams(arity)
      val decoderParams = mkFieldDecoderParams(arity)
      val encoderParams = mkFieldEncoderParams(arity)
      val keys = mkKeys(arity)
      val innerDecoderMap = mkInnerDecoderMap(arity)
      val innerEncoderMap = mkInnerEncoderMap(arity)

      ("""|def Record[T, """ + types + """](""" + keyParams + """) =
          |  new RecordCodecAssoc""" + arity + """(""" + keys + """)
          |
          |class RecordCodecAssoc""" + arity + """(""" + keyParams + """) {
          |  def apply[T, """ + types + """](construct: (""" + types + """) => T, destruct: T => Option[(""" + types + """)])
          |                             (implicit """ + decoderParams + """, """ + encoderParams + """) = {
          |    bindTo(construct, destruct)
          |  }
          |
          |  def bindTo[T, """ + types + """](construct: (""" + types + """) => T, destruct: T => Option[(""" + types + """)])
          |                            (implicit """ + decoderParams + """, """ + encoderParams + """) = {
          |
          |    new RecordCodec""" + arity + """[T, """ + types + """](
          |      construct,
          |      destruct,
          |      """ + keys + """,
          |      """ + mkDecoders(arity) + """,
          |      """ + mkEncoders(arity) + """
          |    )
          |  }
          |}
          |
          |class RecordCodec""" + arity + """[T, """ + types + """](
          |    construct: (""" + types + """) => T,
          |    destruct: T => Option[(""" + types + """)],
          |    """ + keyParams + """,
          |    """ + decoderParams + """,
          |    """ + encoderParams + """) extends base.RecordCodec[T] {
          |
          |  def keys = Set(""" + keys + """)
          |  def innerDecoders = """ + innerDecoderMap + """
          |  def innerEncoders = """ + innerEncoderMap + """
          |
          |  """ + mkRecordDecodeDef(arity) + """
          |  """ + mkRecordEncodeDef(arity) + """
          |  }
          |""").stripMargin
    }

    val classes = 1 to 22 map mkClass mkString "\n\n"

    ("""|package fauna.codex.builder
        |
        |import language.existentials
        |
        |trait RecordCodecConstructors[In, Out] { self: RecordCodecBuilder[In, Out] =>
        |""" + classes + """
        |}
        |""").stripMargin
  }

  def genTupleDecoders = {
    def mkClass(arity: Int) = {

      // in definition
      val types = mkTypes(arity)
      val decoderParams = mkDecoderParams(arity)
      val innerDecoderVec = mkInnerDecoderVec(arity)

      ("""|implicit def tuple""" + arity + """Decoder[""" + types + """](implicit """ + decoderParams + """) =
          |  new Tuple""" + arity + """Decoder[""" + types + """](""" + mkDecoders(
        arity) + """)
          |
          |class Tuple""" + arity + """Decoder[""" + types + """](""" + decoderParams + """)
          |    extends base.TupleDecoder[(""" + types + """)] {
          |
          |  def innerDecoders = """ + innerDecoderVec + """
          |
          |  """ + mkTupleDecodeDef(arity) + """
          |}
          |""").stripMargin
    }

    val classes = 2 to 22 map mkClass mkString "\n\n"

    ("""|package fauna.codex.builder
        |
        |trait TupleDecoderClasses[In] { self: TupleDecoderType[In] =>
        |""" + classes + """
        |}
        |""").stripMargin
  }

  def genTupleEncoders = {
    def mkClass(arity: Int) = {

      // in definition
      val types = mkTypes(arity)
      val encoderParams = mkEncoderParams(arity)
      val innerEncoderSeq = mkInnerEncoderSeq(arity)

      ("""|implicit def tuple""" + arity + """Encoder[""" + types + """](implicit """ + encoderParams + """) =
          |  new Tuple""" + arity + """Encoder[""" + types + """](""" + mkEncoders(
        arity) + """)
          |
          |class Tuple""" + arity + """Encoder[""" + types + """](""" + encoderParams + """)
          |    extends base.TupleEncoder[(""" + types + """)] {
          |
          |  def innerEncoders = """ + innerEncoderSeq + """
          |
          |  """ + mkTupleEncodeDef(arity) + """
          |}
          |""").stripMargin
    }

    val classes = 2 to 22 map mkClass mkString "\n\n"

    ("""|package fauna.codex.builder
        |
        |trait TupleEncoderClasses[Out] { self: TupleEncoderType[Out] =>
        |""" + classes + """
        |}
        |""").stripMargin
  }

}
