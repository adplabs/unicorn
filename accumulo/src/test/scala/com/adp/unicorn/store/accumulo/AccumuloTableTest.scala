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

/**
 * 
 * @author Haifeng Li
 */
class AccumuloSuite extends UnitSpec {
  "Load a non-existing document" should "pass" in new Context {
    table.get("row1")
  }
  
  "Save a document into table" should "pass" in new Context {
    table.put(person)
  }
  
  "Load it back from table" should "pass" in new Context {
    table.get("293050")
  }  
  
  "Load partial document back from table" should "pass" in new Context {
    table.get("293050")
  }  
}