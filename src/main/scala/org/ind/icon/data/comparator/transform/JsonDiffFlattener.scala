package org.ind.icon.data.comparator.transform

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, ValueNode}
import scala.collection.mutable
import scala.util.Try

/**
 * Maps a single field-level discrepancy discovered deep within a nested hierarchy.
 */
case class FieldDiff(column_path: String, source_val: String, target_val: String)

/**
 * A highly optimized JSON traversal engine used as a Spark UDF.
 * It is invoked ONLY on rows that failed the outer-level comparison. It bypasses Spark's 
 * Catalyst schema constraints, allowing infinite nesting and dynamically extracting (path, value) anomalies.
 */
object JsonDiffFlattener extends Serializable {
  
  // Transient lazy initialization ensures the ObjectMapper is instantiated safely on the Worker nodes
  @transient lazy val mapper = new ObjectMapper()

  /**
   * Core logic: Parses Source and Target JSON, flattens both into Map[String, String], 
   * and compares them directly, returning only the nodes that explicitly differ.
   */
  def diffComplexTypes(
    sourceJson: String, targetJson: String, 
    complexTypeKeys: Map[String, Seq[String]], 
    enableNumericTolerance: Boolean, numericTolerance: Double
  ): Seq[FieldDiff] = {
    
    val sMap = if (sourceJson != null) flatten(mapper.readTree(sourceJson), "", complexTypeKeys) else Map.empty[String, String]
    val tMap = if (targetJson != null) flatten(mapper.readTree(targetJson), "", complexTypeKeys) else Map.empty[String, String]

    val diffs = mutable.ListBuffer[FieldDiff]()
    
    (sMap.keySet ++ tMap.keySet).foreach { path =>
      // We use Option to explicitly distinguish between "Key Missing" (None) and "Key Present but Null" (Some(null))
      val sValOpt = sMap.get(path)
      val tValOpt = tMap.get(path)

      if (sValOpt != tValOpt) {
        val sVal = sValOpt.orNull
        val tVal = tValOpt.orNull

        if (enableNumericTolerance && numericTolerance > 0.0 && sVal != null && tVal != null) {
          val sNum = Try(sVal.toDouble).toOption
          val tNum = Try(tVal.toDouble).toOption
          if (sNum.isDefined && tNum.isDefined) {
            if (Math.abs(sNum.get - tNum.get) > numericTolerance) diffs += FieldDiff(path, sVal, tVal)
          } else diffs += FieldDiff(path, sVal, tVal) // Fallback to string diff if NaN
        } else diffs += FieldDiff(path, sVal, tVal)
      }
    }
    diffs.toSeq
  }

  /**
   * Recursively navigates Jackson JSON nodes, constructing dot-notation paths.
   */
  private def flatten(node: JsonNode, path: String, complexKeys: Map[String, Seq[String]]): Map[String, String] = {
    val res = mutable.Map[String, String]()
    
    def traverse(n: JsonNode, p: String): Unit = n match {
      case o: ObjectNode =>
        val it = o.fields()
        while (it.hasNext) { 
          val e = it.next()
          traverse(e.getValue, if(p.isEmpty) e.getKey else s"$p.${e.getKey}") 
        }
      case a: ArrayNode =>
        val baseCol = p.split("\\.").lastOption.getOrElse(p)
        val keyFields = complexKeys.getOrElse(baseCol, Seq.empty)
        
        for (i <- 0 until a.size()) {
          val elem = a.get(i)
          // If the config specified a logical key for this array (e.g. id=5), inject it into the path.
          // Otherwise, fallback to the physical index (e.g. array[0]).
          val eP = if (keyFields.nonEmpty && elem.isObject && elem.has(keyFields.head)) 
            s"$p[${keyFields.head}=${elem.get(keyFields.head).asText()}]" else s"$p[$i]"
          traverse(elem, eP)
        }
      case v: ValueNode => 
        // Explicitly put null values into the map so they don't get lost in the diffing logic
        res.put(p, if (v.isNull) null else v.asText())
      case _ =>
    }
    
    traverse(node, path)
    res.toMap
  }
}