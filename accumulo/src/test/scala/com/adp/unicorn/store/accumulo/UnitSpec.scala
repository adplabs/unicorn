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

package com.adp.unicorn.store.accumulo

import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec

/**
 * 
 * @author Haifeng Li
 */
class UnitSpec extends FlatSpec with BeforeAndAfter {
  trait Context {
    // Turn off debug output
    org.apache.log4j.LogManager.getRootLogger().setLevel(org.apache.log4j.Level.ERROR)
    
    // Connect to Accumulo
    val server = AccumuloServer("local-poc", "127.0.0.1:2181", "tester", "adpadp")
    val table = server.dataset("small", "public")
    
    // Create a document 
    val person = Document("293050")
    person("name") = "Haifeng"
    person("gender") = "Male"
    person("salary") = 1.0
    person("zip") = 10011

    // Create another document.
    // Note the syntax is just like JavaScript
    val address = Document("293050")
    address.street = "135 W. 18th ST"
    address.city = "New York"
    address.state = "NY"
    address.zip = person.zip

    // add a document into another one
    person.address = address
    // add an array into a document
    person.projects = Array("GHCM", "Analytics")

    // Add some relationships
    person("work with", "Jim") = true
    person("work with", "Mike") = true
    person("report to", "Jerome") = true

    // Fetch a non-existing row to warm up the system (loading classes, etc.).
    table.get("row1")
    // Do it twice
    table.get("row1")
  }
}