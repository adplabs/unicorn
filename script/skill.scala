import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.FileWriter
import java.util.zip.GZIPInputStream
import scala.io.Source
import scala.reflect.io.Path
import scala.util.control._

val forLoop = new Breaks
val whileLoop = new Breaks

def gis(s: String) = Source.createBufferedSource(new GZIPInputStream(new BufferedInputStream(new FileInputStream(s))))

val skills = collection.mutable.Map[String, Array[Array[String]]]().withDefaultValue(Array[Array[String]]())
Source.fromFile("/disk2/virtual/data/monster_resumes/skills_sorted.txt").getLines.foreach { line =>
  val words = line.split("\\s+|,")
  skills(words(0)) = skills(words(0)) :+ words
}

skills foreach { case (key, value) => skills(key) = value.sortBy(s => -s.size)}

val fw = new FileWriter("matched.txt")
Path("/disk2/virtual/data/monster_resumes") walkFilter { p => 
  p.isDirectory || """.txt.gz$""".r.findFirstIn(p.name).isDefined
} foreach { file =>
  println(file.path)
  gis(file.path).getLines.foreach { line =>
    val words = line.split("\\s+|,|and").filterNot(_ == "")
    var i = 0
    var skillMatched = 0
    while (i < words.size) {
      if (skills.contains(words(i))) {
        whileLoop.breakable {
          skills(words(i)) foreach { tokens =>
            var matched = true
            if (i + tokens.size - 1 >= words.length) matched = false
            else forLoop.breakable { for (j <- 0 until tokens.size)
              if (words(i+j) != tokens(j)) { matched = false; forLoop.break }
            }

            if (matched) {
              skillMatched = skillMatched + tokens.size
              i = i + tokens.size
              whileLoop.break
            }
          }
          i = i + 1
        }
      } else i = i + 1
    }
    if (skillMatched > 0) fw.write(line+"\n")//println(words.size, skillMatched, 1.0 * skillMatched / words.size, line)
  }
}
fw.close


Path("/disk2/virtual/data/monster_resumes") walkFilter { p => 
  p.isDirectory || """.txt.gz$""".r.findFirstIn(p.name).isDefined
} foreach { file =>
  println(file.path)
  val fw = new FileWriter(file.path.replace(".txt.gz", ".dense.txt"))
  gis(file.path).getLines.foreach { line =>
    val words = line.split("\\s+|,|and").filterNot(_ == "")
    var i = 0
    var skillMatched = 0
    while (i < words.size) {
      if (skills.contains(words(i))) {
        whileLoop.breakable {
          skills(words(i)) foreach { tokens =>
            var matched = true
            if (i + tokens.size - 1 >= words.length) matched = false
            else forLoop.breakable { for (j <- 0 until tokens.size)
              if (words(i+j) != tokens(j)) { matched = false; forLoop.break }
            }

            if (matched) {
              skillMatched = skillMatched + tokens.size
              fw.write(tokens.deep.mkString(" ") + ", ")
              i = i + tokens.size
              whileLoop.break
            }
          }
          i = i + 1
        }
      } else i = i + 1
    }
    if (skillMatched > 0) fw.write("\n")//println(words.size, skillMatched, 1.0 * skillMatched / words.size, line)
  }
  fw.close
}



val skills = collection.mutable.Map[String, Array[Array[String]]]().withDefaultValue(Array[Array[String]]())
Source.fromFile("/disk2/virtual/data/monster_resumes/kosac.txt").getLines.foreach { line =>
  val words = line.split("\\s+|,")
  skills(words(0)) = skills(words(0)) :+ words
}

skills foreach { case (key, value) => skills(key) = value.sortBy(s => -s.size)}

val fw = new FileWriter("skillset.txt")
Path("/disk2/virtual/data/monster_resumes") walkFilter { p => 
  p.isDirectory || """.txt.gz$""".r.findFirstIn(p.name).isDefined
} foreach { file =>
  fw.write(file.name)
  gis(file.path).getLines.foreach { line =>
    val words = line.split("\\s+|,|and").filterNot(_ == "")
    var i = 0
    var skillMatched = 0
    while (i < words.size) {
      if (skills.contains(words(i))) {
        whileLoop.breakable {
          skills(words(i)) foreach { tokens =>
            var matched = true
            if (i + tokens.size - 1 >= words.length) matched = false
            else forLoop.breakable { for (j <- 0 until tokens.size)
              if (words(i+j) != tokens(j)) { matched = false; forLoop.break }
            }

            if (matched) {
              skillMatched = skillMatched + tokens.size
              fw.write(", " + tokens.deep.mkString(" "))
              i = i + tokens.size
              whileLoop.break
            }
          }
          i = i + 1
        }
      } else i = i + 1
    }
  }
  fw.write("\n")
}
fw.close()


val kosac = collection.mutable.Map[String, Int]()
Source.fromFile("/disk2/virtual/data/monster_resumes/skillset_clean.txt").getLines.foreach { line =>
  val words = line.split(",")
  var i = 0
  for (i <- 1 until words.size) {
    val w = words(i).trim
    if (!kosac.contains(w)) kosac(w) = kosac.size + 1
  }
}

val kosacArr = new Array[String](kosac.size + 1)
kosac foreach { case (key, value) => kosacArr(value) = key}

val fw = new FileWriter("trans.txt")
Source.fromFile("/disk2/virtual/data/monster_resumes/skillset_clean.txt").getLines.foreach { line =>
  val words = line.split(",")
  if (words.size > 2) {
    var i = 0
    for (i <- 1 until words.size) {
      val w = words(i).trim
      fw.write(kosac(w) + " ")
    }
    fw.write("\n")
  }
}
fw.close

val fw = new FileWriter("fim.txt")
Source.fromFile("/disk2/virtual/data/monster_resumes/fim/fpgrowth/src/output.fim").getLines.foreach { line =>
  val words = line.split(" ")
  for (i <- 0 until words.size-1) {
      val idx = words(i).trim.toInt
      fw.write(kosacArr(idx) + ", ")
  }
  fw.write(words(words.size-1))
  fw.write("\n")
}
fw.close