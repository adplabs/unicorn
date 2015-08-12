package unicorn.json

/**
 * @author Haifeng Li
 */
trait JsonSerializer {
  /** Serializes a JSON value to a list of key/value pairs, where key is the JSONPath of element. */
  def serialize(value: JsValue, jsonPath: String = "$"): Map[String, Array[Byte]]
  /** Deserialize a JSON value from the given root JSONPath. */
  def deserialize(values: Map[String, Array[Byte]], rootJsonPath: String = "$"): JsValue
}
