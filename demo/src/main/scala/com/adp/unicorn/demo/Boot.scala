package com.adp.unicorn.demo

import scala.concurrent.duration._
import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

/**
 * Created by lihb on 7/16/15.
 */
object Boot extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("unicorn-demo")

  // create and start our service actor
  val service = system.actorOf(Props[SearchDemoServiceActor], "unicorn-demo-service")

  implicit val timeout = Timeout(5.seconds)

  val conf = ConfigFactory.load()
  val serverPort = conf.getInt("spray.can.server.port")

  // start a new HTTP server on port 3801 with our service actor as the handler
  IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = serverPort)
}