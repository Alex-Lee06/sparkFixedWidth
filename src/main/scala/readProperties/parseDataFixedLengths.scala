package readProperties

object parseDataFixedLengths {
  def parseLinePerFixedLengths(line: String, lengths: Seq[Int]): Seq[String] = {
    lengths.indices.foldLeft((line, Array.empty[String])) { case ((rem, fields), idx) =>
      val len = lengths(idx)
      val fld = rem.take(len)
      (rem.drop(len), fields :+ fld)
    }._2
  }
}
