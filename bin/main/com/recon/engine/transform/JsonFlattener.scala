package com.recon.engine.transform

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, ValueNode}
import scala.collection.mutable

/**
 * A robust, JSON-based flattener designed to bypass Spark's strict Catalyst schema constraints.
 * It natively handles infinite nesting, Arrays, and Maps by converting them to flat (path -> value) pairs.
 */
object JsonFlattener extends Serializable {
  
  @transient lazy val mapper = new ObjectMapper()

  def flatten(jsonStr: String, complexTypeKeys: Map[String, Seq[String]]): Seq[(String, String)] = {
    if (jsonStr == null || jsonStr.trim.isEmpty) return Seq.empty
    
    val root = mapper.readTree(jsonStr)
    val result = mutable.ListBuffer[(String, String)]()

    def traverse(node: JsonNode, path: String): Unit = {
      node match {
        case obj: ObjectNode =>
          val it = obj.fields()
          while (it.hasNext) {
            val entry = it.next()
            val newPath = if (path.isEmpty) entry.getKey else s"$path.${entry.getKey}"
            traverse(entry.getValue, newPath)
          }
          
        case arr: ArrayNode =>
          val isKeyed = complexTypeKeys.contains(path)
          val keyFields = if (isKeyed) complexTypeKeys(path) else Seq.empty

          for (i <- 0 until arr.size()) {
            val elem = arr.get(i)
            var elementPath = s"$path[$i]" // Default to index-based

            if (isKeyed && elem.isObject) {
              val keyField = keyFields.head
              if (elem.has(keyField)) {
                val keyValue = elem.get(keyField).asText()
                elementPath = s"$path[$keyField=$keyValue]"
              }
            }
            traverse(elem, elementPath)
          }
          
        case value: ValueNode if !value.isNull =>
          result.append((path, value.asText()))
          
        case _ => // Explicitly ignore nulls during flattening to streamline diff logic
      }
    }

    traverse(root, "")
    result.toSeq
  }
}