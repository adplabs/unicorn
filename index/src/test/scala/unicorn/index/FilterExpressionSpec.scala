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

package unicorn.index

import java.time.Instant
import java.util.Date

import org.specs2.mutable._

class FilterExpressionSpec extends Specification {

  "The FilterExpressionParser" should {
    "parse 'a = 0'" in {
      FilterExpression("a = 0") === Eq("a", IntLiteral(0))
      FilterExpression("0 = a") === Eq("a", IntLiteral(0))
    }
    "parse 'a = 0.1'" in {
      FilterExpression("a = 0.1") === Eq("a", DoubleLiteral(0.1))
    }
    "parse 'a = 1e-1'" in {
      FilterExpression("a = 1e-1") === Eq("a", DoubleLiteral(1e-1))
    }
    "parse 'a = abc'" in {
      FilterExpression("""a = "abc"""") === Eq("a", StringLiteral("abc"))
    }
    "parse 'a = 2015-09-14T04:49:13Z'" in {
      FilterExpression("a = 2015-09-14T04:49:13Z") === Eq("a", DateLiteral(Date.from(Instant.parse("2015-09-14T04:49:13Z"))))
      FilterExpression("a = 2015-09-14T04:49:13.123Z") === Eq("a", DateLiteral(Date.from(Instant.parse("2015-09-14T04:49:13.123Z"))))
    }
    "parse 'a != 0'" in {
      FilterExpression("a != 0") === Ne("a", IntLiteral(0))
      FilterExpression("a <> 0") === Ne("a", IntLiteral(0))
      FilterExpression("0 != a") === Ne("a", IntLiteral(0))
      FilterExpression("0 <> a") === Ne("a", IntLiteral(0))
    }
    "parse 'a > 0'" in {
      FilterExpression("a > 0") === Gt("a", IntLiteral(0))
      FilterExpression("0 < a") === Gt("a", IntLiteral(0))
    }
    "parse 'a >= 0'" in {
      FilterExpression("a >= 0") === Ge("a", IntLiteral(0))
      FilterExpression("0 <= a") === Ge("a", IntLiteral(0))
    }
    "parse 'a < 0'" in {
      FilterExpression("a < 0") === Lt("a", IntLiteral(0))
      FilterExpression("0 > a") === Lt("a", IntLiteral(0))
    }
    "parse 'a <= 0'" in {
      FilterExpression("a <= 0") === Le("a", IntLiteral(0))
      FilterExpression("0 >= a") === Le("a", IntLiteral(0))
    }
    "parse 'a.b = 0'" in {
      FilterExpression("a.b = 0") === Eq("a.b", IntLiteral(0))
    }
    "parse 'a[0].b.c[1] = 0'" in {
      FilterExpression("a[0].b.c[1] = 0") === Eq("a[0].b.c[1]", IntLiteral(0))
    }
    "parse '(a = 0)'" in {
      FilterExpression("(a = 0)") === Eq("a", IntLiteral(0))
    }
    "parse 'a >= 0 && a < 10'" in {
      FilterExpression("a >= 0 && a < 10") === And(Ge("a", IntLiteral(0)), Lt("a", IntLiteral(10)))
      FilterExpression("a >= 0 and a < 10") === And(Ge("a", IntLiteral(0)), Lt("a", IntLiteral(10)))
      FilterExpression("a >= 0 AND a < 10") === And(Ge("a", IntLiteral(0)), Lt("a", IntLiteral(10)))
    }
    "parse 'a >= 0 || a < 10'" in {
      FilterExpression("a >= 0 || a < 10") === Or(Ge("a", IntLiteral(0)), Lt("a", IntLiteral(10)))
      FilterExpression("a >= 0 or a < 10") === Or(Ge("a", IntLiteral(0)), Lt("a", IntLiteral(10)))
      FilterExpression("a >= 0 OR a < 10") === Or(Ge("a", IntLiteral(0)), Lt("a", IntLiteral(10)))
    }
    "parse 'a >= 0 || a < 10 && b = 5'" in {
      FilterExpression("a >= 0 || a < 10 && b = 5") === Or(Ge("a", IntLiteral(0)), And(Lt("a", IntLiteral(10)), Eq("b", IntLiteral(5))))
    }
    "parse '(a >= 0 || a < 10) && b = 5'" in {
      FilterExpression("(a >= 0 || a < 10) && b = 5") === And(Or(Ge("a", IntLiteral(0)), Lt("a", IntLiteral(10))), Eq("b", IntLiteral(5)))
    }
  }
}