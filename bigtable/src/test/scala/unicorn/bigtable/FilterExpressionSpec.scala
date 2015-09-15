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

package unicorn.bigtable

import java.time.Instant
import java.util.Date

import org.specs2.mutable._

class FilterExpressionSpec extends Specification {

  "The FilterExpressionParser" should {
    "parse 'a = 0'" in {
      FilterExpression("a = 0") === EqExpression("a", IntLiteral(0))
      FilterExpression("0 = a") === EqExpression("a", IntLiteral(0))
    }
    "parse 'a = 0.1'" in {
      FilterExpression("a = 0.1") === EqExpression("a", DoubleLiteral(0.1))
    }
    "parse 'a = 1e-1'" in {
      FilterExpression("a = 1e-1") === EqExpression("a", DoubleLiteral(1e-1))
    }
    "parse 'a = abc'" in {
      FilterExpression("""a = "abc"""") === EqExpression("a", StringLiteral("abc"))
    }
    "parse 'a = 2015-09-14T04:49:13Z'" in {
      FilterExpression("a = 2015-09-14T04:49:13Z") === EqExpression("a", DateLiteral(Date.from(Instant.parse("2015-09-14T04:49:13Z"))))
      FilterExpression("a = 2015-09-14T04:49:13.123Z") === EqExpression("a", DateLiteral(Date.from(Instant.parse("2015-09-14T04:49:13.123Z"))))
    }
    "parse 'a != 0'" in {
      FilterExpression("a != 0") === NeExpression("a", IntLiteral(0))
      FilterExpression("a <> 0") === NeExpression("a", IntLiteral(0))
      FilterExpression("0 != a") === NeExpression("a", IntLiteral(0))
      FilterExpression("0 <> a") === NeExpression("a", IntLiteral(0))
    }
    "parse 'a > 0'" in {
      FilterExpression("a > 0") === GtExpression("a", IntLiteral(0))
      FilterExpression("0 < a") === GtExpression("a", IntLiteral(0))
    }
    "parse 'a >= 0'" in {
      FilterExpression("a >= 0") === GeExpression("a", IntLiteral(0))
      FilterExpression("0 <= a") === GeExpression("a", IntLiteral(0))
    }
    "parse 'a < 0'" in {
      FilterExpression("a < 0") === LtExpression("a", IntLiteral(0))
      FilterExpression("0 > a") === LtExpression("a", IntLiteral(0))
    }
    "parse 'a <= 0'" in {
      FilterExpression("a <= 0") === LeExpression("a", IntLiteral(0))
      FilterExpression("0 >= a") === LeExpression("a", IntLiteral(0))
    }
    "parse 'a.b = 0'" in {
      FilterExpression("a.b = 0") === EqExpression("a.b", IntLiteral(0))
    }
    "parse 'a[0].b.c[1] = 0'" in {
      FilterExpression("a[0].b.c[1] = 0") === EqExpression("a[0].b.c[1]", IntLiteral(0))
    }
    "parse 'a >= 0 && a < 10'" in {
      FilterExpression("a >= 0 && a < 10") === AndExpression(Seq(GeExpression("a", IntLiteral(0)), LeExpression("a", IntLiteral(10))))
    }
  }
}