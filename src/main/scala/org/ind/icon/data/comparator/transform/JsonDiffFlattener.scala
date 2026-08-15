package org.ind.icon.data.comparator.transform

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, ValueNode}
import scala.collection.mutable
import scala.util.Try

case class FieldDiff(column_path: String, source_val: String, target_val: String)

object JsonDiffFlattener extends Serializable {
  
  @transient lazy val mapper = new ObjectMapper()

  def diffComplexTypes(
    sourceJson: String, targetJson: String, 
    complexTypeKeys: Map[String, Seq[String]], 
    enableNumericTolerance: Boolean, numericTolerance: Double
  ): Seq[FieldDiff] = {
    
    val sMap = if (sourceJson != null) flatten(mapper.readTree(sourceJson), "", complexTypeKeys) else Map.empty[String, String]
    val tMap = if (targetJson != null) flatten(mapper.readTree(targetJson), "", complexTypeKeys) else Map.empty[String, String]

    val diffs = mutable.ListBuffer[FieldDiff]()
    
    (sMap.keySet ++ tMap.keySet).foreach { path =>
      // Use Option to explicitly distinguish between "Key Missing" (None) and "Key Present but Null" (Some(null))
      val sValOpt = sMap.get(path)
      val tValOpt = tMap.get(path)

      if (sValOpt != tValOpt) {
        val sVal = sValOpt.orNull
        val tVal = tValOpt.orNull

        if (enableNumericTolerance && numericTolerance > 0.0 && sVal != null && tVal != null) {
          // Use BigDecimal to strictly preserve native precision
          val sNum = Try(BigDecimal(sVal)).toOption
          val tNum = Try(BigDecimal(tVal)).toOption
          
          if (sNum.isDefined && tNum.isDefined) {
            if ((sNum.get - tNum.get).abs > BigDecimal(numericTolerance)) {
              diffs += FieldDiff(path, sVal, tVal)
            }
          } else diffs += FieldDiff(path, sVal, tVal)
        } else diffs += FieldDiff(path, sVal, tVal)
      }
    }
    diffs.toSeq
  }

  private def flatten(node: JsonNode, path: String, complexKeys: Map[String, Seq[String]]): Map[String, String] = {
    val res = mutable.Map[String, String]()
    def traverse(n: JsonNode, p: String): Unit = n match {
      case o: ObjectNode =>
        val it = o.fields()
        while (it.hasNext) { val e = it.next(); traverse(e.getValue, if(p.isEmpty) e.getKey else s"$p.${e.getKey}") }
      case a: ArrayNode =>
        val baseCol = p.split("\\.").lastOption.getOrElse(p)
        val keyFields = complexKeys.getOrElse(baseCol, Seq.empty)
        for (i <- 0 until a.size()) {
          val elem = a.get(i)
          val eP = if (keyFields.nonEmpty && elem.isObject && elem.has(keyFields.head)) 
            s"$p[${keyFields.head}=${elem.get(keyFields.head).asText()}]" else s"$p[$i]"
          traverse(elem, eP)
        }
      case v: ValueNode => 
        // Explicitly put null values into the map so they are tracked as Some(null)
        res.put(p, if (v.isNull) null else v.asText())
      case _ =>
    }
    traverse(node, path)
    res.toMap
  }
}