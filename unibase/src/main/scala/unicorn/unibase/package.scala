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

/**
 * @author Haifeng Li
 */
package object unibase {
  // Originally we used "." as delimiter in table name.
  // However, "." cannot be part of table name in Accumulo.
  // So we switch to "_".
  private[unibase] val BucketMetaTableName = "unicorn_meta_bucket"
  private[unibase] val BucketMetaTableColumnFamily = "meta"

  private[unibase] val DefaultLocalityField = "default_locality"
}
