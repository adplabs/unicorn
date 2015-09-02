/*******************************************************************************
 * (C) Copyright 2015 ADP, LLC.
 *   
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package unicorn.util

import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory

/**
 * @author Haifeng Li
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
