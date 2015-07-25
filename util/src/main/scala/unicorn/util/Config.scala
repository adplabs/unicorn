package unicorn.util

import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory

/**
 * Created by lihb on 3/20/15.
 */
object Config {
  // a util function, returns development or production or local
  // environment can be passed as jvm args and read from System properties
  def deployEnvironment =
    System.getProperty("unicorn.env", "local")

  def config = {
    val configNamespace = "unicorn"
    val mergedCfg = ConfigFactory.load().getConfig(configNamespace)
    mergedCfg.getConfig(deployEnvironment)
  }

  val isoDateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
  val isoDateTimeMSFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
  val simpleDateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
}
