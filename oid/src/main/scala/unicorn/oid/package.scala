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

package unicorn

import java.util.{Date, UUID}

/**
 * @author Haifeng Li
 */
package object oid {
  implicit def int2ObjectId(x: Int): ObjectId = ObjectId(x)
  implicit def long2ObjectId(x: Long): ObjectId = ObjectId(x)
  implicit def string2ObjectId(x: String): ObjectId = ObjectId(x)
  implicit def date2ObjectId(x: Date): ObjectId = ObjectId(x)
  implicit def uuid2ObjectId(x: UUID): ObjectId = ObjectId(x)
  implicit def bytes2ObjectId(x: Array[Byte]): ObjectId = ObjectId(x)
}
