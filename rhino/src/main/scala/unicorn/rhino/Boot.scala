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

package unicorn.rhino

import scala.concurrent.duration._
import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

/**
 * @author Haifeng Li
 */
object Boot extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("unicorn-rhino")

  // create and start our service actor
  val service = system.actorOf(Props[RhinoActor], "unicorn-rhino")

  implicit val timeout = Timeout(5.seconds)

  val conf = ConfigFactory.load()
  val serverPort = conf.getInt("spray.can.server.port")

  val ip = if (System.getProperty("loopback.only") != null) "127.0.0.1" else "0.0.0.0"
  IO(Http) ? Http.Bind(service, interface = ip, port = serverPort)
}
