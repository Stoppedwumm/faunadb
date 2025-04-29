package fauna.model.test

import fauna.model.runtime.fql2.QueryCheckFailure
import fauna.repo.values.Value
import fql.ast.{ Expr, Span, Src }
import fql.error.{ Hint, Log, TypeError, Warning }

// This has a bit of overlap with the core checker spec at fql/TyperCoreSpec. As
// the model-level type tests expand, they should focus on environment
// integration.
class FQL2TypecheckSpec extends FQL2Spec {
  import Hint.HintType

  "interpreter typechecks" - {
    val auth = newAuth

    evalOk(
      auth,
      """|Collection.create({
         |  name: "Author",
         |  indexes: {
         |    byName: { terms: [{ field: "name" }] }
         |  }
         |})""".stripMargin
    )

    val doc =
      evalOk(auth, "Author.create({ name: 'Alice' })").asInstanceOf[Value.Doc]
    val id = doc.id.subID.toLong

    "can check trivial types" in {
      val res1 = evalRes(auth, "2")
      res1.value shouldEqual Value.Number(2)
      res1.typeStr shouldEqual "2"

      val res2 = evalRes(auth, "x => x")
      res2.value should matchPattern {
        case Value.Lambda(Seq(Some("x")), None, Expr.Id("x", _), ctx)
            if ctx.isEmpty =>
      }
      res2.typeStr shouldEqual "A => A"

      val res3 = evalRes(auth, "{ a: 'foo' }")
      res3.value shouldEqual Value.Struct("a" -> Value.Str("foo"))
      res3.typeStr shouldEqual """{ a: "foo" }"""
    }

    "can return trivial errors" in {
      evalErr(auth, "{ a: 1 }.b") should matchPattern {
        case QueryCheckFailure(Seq(
              TypeError("Type `{ a: 1 }` does not have field `b`", _, Nil, Nil))) =>
      }
    }

    "types top-level built-ins" in {
      evalRes(auth, "Math").typeStr shouldEqual "MathModule"

      // FIXME: should be overloaded w/ specific number type returns:
      // Int => Int & Long => Long & Double => Double
      evalRes(auth, "Math.abs").typeStr shouldEqual "(x: Number) => Number"

      // FIXME: needs Ordering alias
      evalRes(
        auth,
        "asc").typeStr shouldEqual "(accessor: (A => Any)) => { asc: A => Any }"

      // FIXME: needs Ordering as above
      evalRes(
        auth,
        "FQL.asc").typeStr shouldEqual "(accessor: (A => Any)) => { asc: A => Any }"

      // check annotation
      evalRes(auth, "let m: MathModule = Math; m").typeStr shouldEqual "MathModule"
    }

    "types user-defined collections" in {
      evalRes(auth, "Author").typeStr shouldEqual "AuthorCollection"
      evalRes(auth, "Author == 1").typeStr shouldEqual "Boolean"
      evalRes(auth, "Author.all()").typeStr shouldEqual "Set<Author>"

      evalRes(
        auth,
        "Author.byName").typeStr shouldEqual "((term1: Any) => Set<Author>) & ((term1: Any, range: { from: Any } | { to: Any } | { from: Any, to: Any }) => Set<Author>)"

      // check annotations
      evalRes(
        auth,
        "let a: AuthorCollection = Author; a").typeStr shouldEqual "AuthorCollection"

      // check annotations
      evalRes(
        auth,
        "let a: Set<Author> = Author.all(); a").typeStr shouldEqual "Set<Author>"
    }

    "types updates with refs" in {
      val auth = newDB
      updateSchemaOk(
        auth,
        "test.fsl" ->
          """|collection Foo {
             |  x: String
             |}
             |
             |collection Bar {
             |  foo: Ref<Foo>
             |  index byFoo {
             |    terms [.foo]
             |  }
             |}
             |
             |function updateABar(foo: Foo): Null {
             |  let set: Set<Bar> = Bar.byFoo(foo)
             |  set.forEach(bar => { bar.update({ foo: foo }) })
             |}""".stripMargin
      )

      updateSchemaOk(
        auth,
        "test.fsl" ->
          """|collection Foo {
             |  x: String
             |}
             |
             |collection Bar {
             |  foo: Ref<Foo>
             |  index byFoo {
             |    terms [.foo]
             |  }
             |}
             |
             |function getASet(f: Ref<Foo>): Set<Bar> {
             |  Bar.byFoo(f)
             |}
             |
             |function updateABar(foo: Foo): Null {
             |  getASet(foo).forEach(bar => { bar.update({ foo: foo }) })
             |}""".stripMargin
      )
    }

    "typechecks fields" in {
      evalRes(auth, "'foo'.length").typeStr shouldEqual "Number"
    }

    "typechecks strings" in {
      evalRes(auth, "\"abc\"").typeStr shouldEqual "\"abc\""
      evalRes(auth, "\"abc #{2}\"").typeStr shouldEqual "String"
      evalRes(auth, "'foo'.slice(1, 2)").typeStr shouldEqual "String"
      evalRes(auth, "'foo'.startsWith('bar')").typeStr shouldEqual "Boolean"

      // check annotations
      evalRes(auth, "let s: String = \"abc\"; s").typeStr shouldEqual "String"

      evalErr(auth, "\"abc #{'a'['b']}\"") should matchPattern {
        case QueryCheckFailure(
              Seq(
                TypeError(
                  "Type `String` is not a subtype of `Number`",
                  _,
                  Nil,
                  Nil))) =>
      }
    }

    "typechecks booleans" in {
      evalRes(auth, "true || false").typeStr shouldEqual "Boolean"
      evalRes(auth, "true || false && true").typeStr shouldEqual "Boolean"

      evalErr(auth, "true || 3") should matchPattern {
        case QueryCheckFailure(Seq(
              TypeError("Type `Int` is not a subtype of `Boolean`", _, Nil, Nil))) =>
      }

      // check annotations
      evalRes(
        auth,
        "let b: Boolean = true || false; b").typeStr shouldEqual "Boolean"
    }

    // TODO: narrow type to int, long, etc.
    "typechecks numbers" in {
      evalRes(auth, "[1, 2, 3].map(x => x + 1)").typeStr shouldEqual "Array<Number>"
      evalRes(auth, "[1, 2, 3].map(x => x - 1)").typeStr shouldEqual "Array<Number>"
      evalRes(auth, "[1, 2, 3].map(x => -x)").typeStr shouldEqual "Array<Number>"
    }

    "typechecks projection" in {
      // structs
      evalRes(auth, "({ foo: 1, bar: 2 }) { foo }").typeStr shouldEqual "{ foo: 1 }"

      // arrays
      val foos = "[1, 2, 3].map(x => { foo: x + 1 })"
      evalRes(auth, s"$foos { foo }").typeStr shouldEqual "Array<{ foo: Number }>"

      evalRes(
        auth,
        s"($foos { foo }).first()").typeStr shouldEqual "{ foo: Number } | Null"

      // set
      evalRes(auth, "Author.all() { name }").typeStr shouldEqual "Set<{ name: Any }>"
      evalRes(
        auth,
        "(Author.all() { name }).first()").typeStr shouldEqual "{ name: Any } | Null"
      evalRes(
        auth,
        "(Author.all() { name }).toArray()").typeStr shouldEqual "Array<{ name: Any }>"

      // document lookups
      evalRes(
        auth,
        s"Author.byId('$id')! { name }").typeStr shouldEqual "{ name: Any }"
      evalRes(
        auth,
        s"Author.byId('$id') { name }").typeStr shouldEqual "{ name: Any } | Null"
    }

    "typechecks arrays" in {
      evalRes(
        auth,
        "[1 + 1, 2 + 1, 3 + 1]").typeStr shouldEqual "[Number, Number, Number]"
      evalRes(auth, "[1, 2, 3].map(x => x + 1)").typeStr shouldEqual "Array<Number>"

      // projection works
      evalRes(
        auth,
        """|[1, 2, 3].map(x => {
           |  { foo: x + 1, bar: x + 1 }
           |}) { foo }""".stripMargin).typeStr shouldEqual "Array<{ foo: Number }>"

      // check that flatMap is typed correctly
      evalRes(
        auth,
        """|["a", "abc", "ab"].flatMap(x => [x.length, 0])
           |""".stripMargin).typeStr shouldEqual "Array<Number>"

      // check that fold is typed correctly
      evalRes(
        auth,
        """|["bcd", "efgh", "ijk"].fold(1, (x, y) => x + y.length)
           |""".stripMargin).typeStr shouldEqual "Number"

      // check that reduce is typed correctly
      evalRes(
        auth,
        """|["bcd", "efgh", "ijk"].reduce((x, y) => x + y)
           |""".stripMargin).typeStr shouldEqual "String | Null"
    }

    "typechecks sets" in {
      evalRes(auth, "Author.all()").typeStr shouldEqual "Set<Author>"
      evalRes(auth, "Author.all() { id }").typeStr shouldEqual "Set<{ id: ID }>"
      evalRes(auth, "Author.all().first()").typeStr shouldEqual "Author | Null"
      evalRes(auth, "Author.all().first()?.id").typeStr shouldEqual "ID | Null"
    }

    "typechecks comparison operators" in {
      evalRes(auth, "2 == 3").typeStr shouldEqual "Boolean"
      evalRes(auth, "2 != 3").typeStr shouldEqual "Boolean"
      evalRes(auth, "2 > 3").typeStr shouldEqual "Boolean"
      evalRes(auth, "2 < 3").typeStr shouldEqual "Boolean"
      evalRes(auth, "2 >= 3").typeStr shouldEqual "Boolean"
      evalRes(auth, "2 <= 3").typeStr shouldEqual "Boolean"
    }

    "typechecks math" in {
      // Ideally this returns an int, but that's difficult
      evalRes(auth, "2 / 3").typeStr shouldEqual "Number"
      evalRes(auth, "2 + 3").typeStr shouldEqual "Number"
      evalRes(auth, "2.0 + 3.0").typeStr shouldEqual "Number"

      evalErr(auth, "2 + 'a'") should matchPattern {
        case QueryCheckFailure(
              Seq(
                TypeError(
                  "Type `String` is not a subtype of `Number`",
                  _,
                  Nil,
                  Nil))) =>
      }
    }

    "typechecks returns from static functions" in {
      evalRes(auth, "Time.now()").typeStr shouldEqual "Time"
    }

    "typechecks args to apply functions" in {
      val err1 = evalErr(auth, "Time(3)")
      err1 should matchPattern {
        case QueryCheckFailure(Seq(
              TypeError("Type `Int` is not a subtype of `String`", _, Nil, Nil))) =>
      }

      val err2 = evalErr(auth, "Math(3)")
      err2 should matchPattern {
        case QueryCheckFailure(
              Seq(
                TypeError(
                  "Type `MathModule` cannot be used as a function",
                  _,
                  Nil,
                  Seq(
                    Hint(
                      "`MathModule` is used as a function here",
                      Span(4, 7, _),
                      None,
                      HintType.General))))) =>
      }
    }

    "typechecks args to static functions" in {
      inside(evalErr(auth, "Time.fromString(3)")) {
        case QueryCheckFailure(Seq(TypeError(msg, _, Nil, Nil))) =>
          msg shouldEqual "Type `Int` is not a subtype of `String`"
      }
    }

    "typechecks args to select" in {
      // ensures the freeVars finds values in Select
      inside(evalErr(auth, "[][Author]")) {
        case QueryCheckFailure(Seq(TypeError(msg, _, Nil, Nil))) =>
          msg shouldEqual "Type `AuthorCollection` is not a subtype of `Number`"
      }
    }

    "warns about uncalled functions" in {
      val out = eval(auth, "Time.now")

      val res = out.res.getOrElse(fail())
      res.typeStr shouldEqual "() => Time"

      val span = Span(0, 8, Src.Query(""))
      out.logs.check shouldBe Seq(
        Warning(
          "Function is not called.",
          span,
          hints = Seq(
            Hint("Call the function.", span.copy(start = span.end), Some("()")))))
      out.logs.runtime shouldBe Seq.empty
    }

    "warns about uncalled overloaded functions" in {
      val out = eval(auth, "(x, y) => x + y")

      val res = out.res.getOrElse(fail())
      res.typeStr shouldEqual "((Number, Number) => Number) & ((String, String) => String)"

      val span = Span(0, 15, Src.Query(""))
      out.logs.check shouldBe Seq(
        Warning(
          "Function is not called.",
          span,
          hints = Seq(
            Hint("Call the function.", span.copy(start = span.end), Some("()")))))
      out.logs.runtime shouldBe Seq.empty
    }

    "dbg does types correctly" in {
      val out = eval(auth, "dbg(1 + 2) + 10")

      val res = out.res.getOrElse(fail())
      res.value shouldBe Value.Number(13)
      res.typeStr shouldBe "Number"

      out.logs.check shouldBe Seq.empty
      out.logs.runtime shouldBe Seq(
        Log("3", Span(3, 10, Src.Query("")), long = true))
    }

    "log does types correctly" in {
      val out1 = eval(auth, "log('hello')")
      val res1 = out1.res.getOrElse(fail())
      val span1 = Span(3, 12, Src.Query(""))

      res1.value shouldBe Value.Null(span1)
      res1.typeStr shouldBe "Null"
      out1.logs.check shouldBe Seq.empty
      out1.logs.runtime shouldBe Seq(Log("hello", span1, long = false))

      val out2 = eval(auth, "log('foo', 3)")
      val res2 = out2.res.getOrElse(fail())
      val span2 = Span(3, 13, Src.Query(""))

      res2.value shouldBe Value.Null(span2)
      res2.typeStr shouldBe "Null"
      out2.logs.check shouldBe Seq.empty
      out2.logs.runtime shouldBe Seq(Log("foo 3", span2, long = false))

      val out3 = eval(auth, "log()")
      val res3 = out3.res.getOrElse(fail())
      val span3 = Span(3, 5, Src.Query(""))

      res3.value shouldBe Value.Null(span3)
      res3.typeStr shouldBe "Null"
      out3.logs.check shouldBe Seq.empty
      out3.logs.runtime shouldBe Seq(Log("", span3, long = false))
    }
  }

  "udf" - {
    val auth = newDB

    "inferred type location is not shown" in {
      evalOk(auth, "Function.create({ name: 'AddOne', body: 'x => x + 1' })")

      evalErr(auth, "AddOne(2).foo") should matchPattern {
        case QueryCheckFailure(Seq(
              TypeError("Type `Number` does not have field `foo`", _, Nil, Nil))) =>
      }
    }
  }

  "typechecking should not 500" in {
    val auth = newDB

    val q = (0 until 20000).mkString(" + ")
    evalErr(auth, q) shouldBe QueryCheckFailure.TypecheckStackOverflow(
      Span(0, q.length, Src.Query("")))
  }

  "invalid projection" in {
    val auth = newDB

    val err = evalErr(auth, "Function.create({ name: 'Bar', body: 'x => x { x }' })")

    err.errors.map(_.renderWithSource(Map.empty)) shouldBe Seq(
      """|error: Invalid database schema update.
         |    error: Invalid projection on a unknown type. Use function signatures to fix the issue.
         |    at main.fsl:5:3
         |      |
         |    5 |     x {
         |      |  ___^
         |    6 | |     x: .x
         |    7 | |   }
         |      | |___^
         |      |
         |at *query*:1:16
         |  |
         |1 | Function.create({ name: 'Bar', body: 'x => x { x }' })
         |  |                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "function signature fix invalid projection" in {
    val auth = newDB

    evalOk(
      auth,
      """|Function.create({
         |  name: "Bar",
         |  signature: "{ x: a } => { x: a }",
         |  body: 'x => x { x }'
         |})""".stripMargin
    )
  }

  "lookup env for nested calls" in {
    val auth = newDB

    evalOk(auth, "Collection.create({ name: 'Foo' })")
    evalOk(auth, "Function.create({ name: 'Bar', body: '() => Foo.all()' })")
    evalRes(auth, "Bar()").typeStr shouldBe "Set<Foo>"
    evalRes(auth, "Bar() { id }").typeStr shouldBe "Set<{ id: ID }>"
  }

  "lookup env for type annotations" in {
    val auth = newDB

    evalOk(auth, "Collection.create({ name: 'Foo' })")

    evalRes(auth, "let a: Any = null; let b: Ref<Foo> = a; b")
  }

  "lookup env for null doc types" in {
    val auth = newDB

    evalOk(auth, "Collection.create({ name: 'Foo' })")
    evalOk(
      auth,
      """|Function.create({
         |  name: "Bar",
         |  signature: "() => NullFoo",
         |  body: "() => { let v: Any = Foo.byId('0'); v }"
         |})""".stripMargin
    )
    evalRes(auth, "Bar()").typeStr shouldBe "NullFoo"
    evalRes(auth, "Bar().id").typeStr shouldBe "ID"
  }

  "lookup env for collection types" in {
    val auth = newDB

    evalOk(auth, "Collection.create({ name: 'Foo' })")
    evalOk(
      auth,
      """|Function.create({
         |  name: "Bar",
         |  body: "() => Foo"
         |})""".stripMargin
    )
    evalRes(auth, "Bar()").typeStr shouldBe "FooCollection"
    evalRes(auth, "Bar().definition").typeStr shouldBe "CollectionDef"
  }

  "lookup env for computed fields" in {
    val auth = newDB

    evalOk(auth, "Collection.create({ name: 'Foo' })")
    evalOk(
      auth,
      """|Collection.create({
         |  name: "Bar",
         |  computed_fields: {
         |    foo: {
         |      body: "_ => Foo.all()"
         |    }
         |  }
         |})""".stripMargin
    )
    evalOk(auth, "Bar.create({ id: '1' })")

    evalRes(auth, "Bar.byId('1')!.foo").typeStr shouldBe "Set<Foo>"
    evalRes(auth, "Bar.byId('1')!.foo { id }").typeStr shouldBe "Set<{ id: ID }>"
  }

  "demo data works" in {
    val auth = newDB

    updateSchemaOk(
      auth,
      "collections.fsl" ->
        """|collection Customer {
           |  name: String
           |  email: String
           |  address: {
           |    street: String,
           |    city: String,
           |    state: String,
           |    postalCode: String,
           |    country: String
           |  }
           |
           |  compute cart: Order? = (customer => Order.byCustomerAndStatus(customer, 'cart').first())
           |
           |  // Use a computed field to get the set of Orders for a customer.
           |  compute orders: Set<Order> = ( customer => Order.byCustomer(customer))
           |
           |  // Use a unique constraint to ensure no two customers have the same email.
           |  unique [.email]
           |
           |  index byEmail {
           |    terms [.email]
           |  }
           |}
           |
           |collection Product {
           |  name: String
           |  description: String
           |  price: Number
           |  category: Ref<Category>
           |  stock: Int
           |
           |  // Use a unique constraint to ensure no two products have the same name.
           |  unique [.name]
           |  check stockIsValid (product => product.stock >= 0)
           |  check priceIsValid (product => product.price >= 0)
           |
           |  index byCategory {
           |    terms [.category]
           |  }
           |
           |  index sortedByCategory {
           |    values [.category]
           |  }
           |
           |  index byName {
           |    terms [.name]
           |  }
           |
           |  index sortedByPriceLowToHigh {
           |    values [.price, .name, .description, .stock]
           |  }
           |}
           |
           |collection Category {
           |  name: String
           |  description: String
           |  compute products: Set<Product> = (category => Product.byCategory(category))
           |
           |  unique [.name]
           |
           |  index byName {
           |    terms [.name]
           |  }
           |}
           |
           |collection Order {
           |  customer: Ref<Customer>
           |  status: "cart" | "processing" | "shipped" | "delivered"
           |  createdAt: Time
           |
           |  compute items: Set<OrderItem> = (order => OrderItem.byOrder(order))
           |  compute total: Number = (order => order.items.fold(0, (sum, orderItem) => {
           |    let orderItem: Any = orderItem
           |    if (orderItem.product != null) {
           |      sum + orderItem.product.price * orderItem.quantity
           |    } else {
           |      sum
           |    }
           |  }))
           |  payment: { *: Any }
           |
           |  check oneOrderInCart (order => {
           |    Order.byCustomerAndStatus(order.customer, "cart").count() <= 1
           |  })
           |
           |  // Define an index to get all orders for a customer. Orders will be sorted by
           |  // createdAt in descending order.
           |  index byCustomer {
           |    terms [.customer]
           |    values [desc(.createdAt), .status]
           |  }
           |
           |  index byCustomerAndStatus {
           |    terms [.customer, .status]
           |  }
           |}
           |
           |collection OrderItem {
           |  order: Ref<Order>
           |  product: Any
           |  quantity: Int
           |
           |  unique [.order, .product]
           |  check positiveQuantity (orderItem => orderItem.quantity > 0)
           |
           |  index byOrder {
           |    terms [.order]
           |    values [.product, .quantity]
           |  }
           |
           |  index byOrderAndProduct {
           |    terms [.order, .product]
           |  }
           |}
           |""".stripMargin,
      "functions.fsl" ->
        """|function createOrUpdateCartItem(customerId, productName, quantity) {
           |  // Find the customer by id, using the ! operator to assert that the customer exists.
           |  // If the customer does not exist, fauna will throw a document_not_found error.
           |  let customer = Customer.byId(customerId)!
           |  // There is a unique constraint on [.name] so this will return at most one result.
           |  let product = Product.byName(productName).first()
           |
           |  // Check if the product exists.
           |  if (product == null) {
           |    abort("Product does not exist.")
           |  }
           |
           |  // Check that the quantity is valid.
           |  if (quantity < 0) {
           |    abort("Quantity must be a non-negative integer.")
           |  }
           |
           |  // Create a new cart for the customer if they do not have one.
           |  if (customer!.cart == null) {
           |    Order.create({
           |      status: "cart",
           |      customer: customer,
           |      createdAt: Time.now(),
           |      payment: {}
           |    })
           |  }
           |
           |  // Check that the product has the requested quantity in stock.
           |  if (product!.stock < quantity) {
           |    abort("Product does not have the requested quantity in stock.")
           |  }
           |
           |  // Attempt to find an existing order item for the order, product pair.
           |  // There is a unique constraint on [.order, .product] so this will return at most one result.
           |  let orderItem = OrderItem.byOrderAndProduct(customer!.cart, product).first()
           |
           |  if (orderItem == null) {
           |    // If the order item does not exist, create a new one.
           |    // OrderItem.create({
           |    //   order: Order(customer!.cart!.id),
           |    //   product: product,
           |    //   quantity: quantity,
           |    // })
           |    orderItem!.update({ quantity: quantity })
           |  } else {
           |    // If the order item exists, update the quantity.
           |    orderItem!.update({ quantity: quantity })
           |  }
           |}
           |
           |function getOrCreateCart(id) {
           |  // Find the customer by id, using the ! operator to assert that the customer exists.
           |  // If the customer does not exist, fauna will throw a document_not_found error.
           |  let customer = Customer.byId(id)!
           |
           |  if (customer!.cart == null) {
           |    // Create a cart if the customer does not have one.
           |    Order.create({
           |      status: 'cart',
           |      customer: Customer.byId(id),
           |      createdAt: Time.now(),
           |      payment: {}
           |    })
           |    customer!.cart
           |  } else {
           |    // Return the cart if it already exists.
           |    customer!.cart
           |  }
           |}
           |
           |function checkout(orderId, status, payment) {
           |  // Find the order by id, using the ! operator to assert that the order exists.
           |  let order = Order.byId(orderId)!
           |
           |  // Check that we are setting the order to the processing status. If not, we should
           |  // not be calling this function.
           |  if (status != "processing") {
           |    abort("Can not call checkout with status other than processing.")
           |  }
           |
           |  // Check that the order can be transitioned to the processing status.
           |  validateOrderStatusTransition(order!.status, "processing")
           |
           |  // Check that the order has at least one order item.
           |  if (order!.items.isEmpty()) {
           |    abort("Order must have at least one item.")
           |  }
           |
           |  // Check that customer has a valid address.
           |  if (order!.customer!.address == null) {
           |    abort("Customer must have a valid address.")
           |  }
           |
           |  // Check that the order has a payment method if not provided as an argument.
           |  if (order!.payment == null && payment == null) {
           |    abort("Order must have a valid payment method.")
           |  }
           |
           |  // Check that the order items are still in stock.
           |  order!.items.forEach((item) => {
           |    let product: Any = item.product
           |    if (product.stock < item.quantity) {
           |      abort("One of the selected products does not have the requested quantity in stock.")
           |    }
           |  })
           |
           |  // Decrement the stock of each product in the order.
           |  order!.items.forEach((item) => {
           |    let product: Any = item.product
           |    product.update({ stock: product.stock - item.quantity })
           |  })
           |
           |  // Transition the order to the processing status, update the payment if provided.
           |  if (payment != null) {
           |    order!.update({ status: "processing", payment: payment })
           |  } else {
           |    order!.update({ status: "processing" })
           |  }
           |}
           |
           |function validateOrderStatusTransition(oldStatus, newStatus) {
           |  if (oldStatus == "cart" && newStatus != "processing") {
           |    // The order can only transition from cart to processing.
           |    abort("Invalid status transition.")
           |  } else if (oldStatus == "processing" && newStatus != "shipped") {
           |    // The order can only transition from processing to shipped.
           |    abort("Invalid status transition.")
           |  } else if (oldStatus == "shipped" && newStatus != "delivered") {
           |    // The order can only transition from shipped to delivered.
           |    abort("Invalid status transition.")
           |  }
           |}
           |""".stripMargin
    )
  }

  "array with no generics doesn't blow up" in {
    val auth = newDB

    renderErr(
      auth,
      """|Collection.create({
         |  name: "User",
         |  fields: {
         |    my_field: { signature: "Array" }
         |  }
         |})""".stripMargin
    ) shouldBe (
      """|error: Invalid database schema update.
         |    error: Type constructor `Array<>` accepts 1 parameters, received 0
         |    at main.fsl:5:13
         |      |
         |    5 |   my_field: Array
         |      |             ^^^^^
         |      |
         |at *query*:1:18
         |  |
         |1 |   Collection.create({
         |  |  __________________^
         |2 | |   name: "User",
         |3 | |   fields: {
         |4 | |     my_field: { signature: "Array" }
         |5 | |   }
         |6 | | })
         |  | |__^
         |  |""".stripMargin
    )
  }

  "array with no generics and an inline index build should not blow up" in {
    // This requires some delicate coordination:
    // - First, the updated collection definition is written. We cannot check the
    //   `Array` signature in a field validator, as we do not know user-defined types
    //   at that time.
    // - Next, the type env validator is run. This generates schema, which is then
    //   passed through the schema type validator. Note that schema must be able to
    //   be generated with the invalid signature shown here.
    // - The schema type validator then emits the error seen below.
    // - The process ends after that error, but the very next step (had it succeeded)
    //   would be to build inline indexes. Building inline indexes will 500 if given
    //   the below document, as the field definition cannot be parsed.

    val auth = newDB

    renderErr(
      auth,
      """|Collection.create({
         |  name: "User",
         |  fields: {
         |    my_field: { signature: "Array" }
         |  },
         |  indexes: {
         |    byFoo: { terms: [{ field: ".foo" }] }
         |  }
         |})""".stripMargin
    ) shouldBe (
      """|error: Invalid database schema update.
         |    error: Type constructor `Array<>` accepts 1 parameters, received 0
         |    at main.fsl:5:13
         |      |
         |    5 |   my_field: Array
         |      |             ^^^^^
         |      |
         |    error: Type `User` does not have field `foo`
         |    at main.fsl:7:13
         |      |
         |    7 |     terms [.foo]
         |      |             ^^^
         |      |
         |at *query*:1:18
         |  |
         |1 |   Collection.create({
         |  |  __________________^
         |2 | |   name: "User",
         |3 | |   fields: {
         |4 | |     my_field: { signature: "Array" }
         |5 | |   },
         |6 | |   indexes: {
         |7 | |     byFoo: { terms: [{ field: ".foo" }] }
         |8 | |   }
         |9 | | })
         |  | |__^
         |  |""".stripMargin
    )
  }
}

class FQL2TypecheckWithV4Spec extends FQL2WithV4Spec {
  "v4 functions typecheck correctly" in {
    val auth = newDB

    evalV4Ok(
      auth,
      CreateFunction(
        MkObject("name" -> "Foo", "body" -> QueryF(Lambda(Seq("x") -> Var("x"))))))

    evalOk(auth, "Function.create({ name: 'Bar', body: 'x => Foo(x)' })")

    evalOk(auth, "Bar(3)") shouldBe Value.Int(3)
  }

  "v4 functions with conflicting names work" in {
    val auth = newDB

    evalV4Ok(
      auth,
      CreateFunction(
        MkObject("name" -> "foo", "body" -> QueryF(Lambda(Seq("x") -> Var("x"))))))
    evalV4Ok(auth, CreateCollection(MkObject("name" -> "foo")))

    // Collections take priority over functions.
    renderErr(
      auth,
      "Function.create({ name: 'Bar', body: 'x => foo.zzz(x)' })") shouldBe (
      """|error: Invalid database schema update.
         |    error: Type `fooCollection` does not have field `zzz`
         |    at main.fsl:8:7
         |      |
         |    8 |   foo.zzz(x)
         |      |       ^^^
         |      |
         |    hint: Type `fooCollection` inferred here
         |    at main.fsl:4:12
         |      |
         |    4 | collection foo {
         |      |            ^^^
         |      |
         |at *query*:1:16
         |  |
         |1 | Function.create({ name: 'Bar', body: 'x => foo.zzz(x)' })
         |  |                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }

  "v4 collections with aliases should still take priority" in {
    val auth = newDB

    evalV4Ok(
      auth,
      CreateFunction(
        MkObject("name" -> "foo", "body" -> QueryF(Lambda(Seq("x") -> Var("x"))))))
    evalV4Ok(auth, CreateCollection(MkObject("name" -> "foo")))

    evalOk(auth, "Collection.byName('foo')!.update({ alias: 'MyColl' })")

    // Even with an alias, the collection takes priority.
    renderErr(
      auth,
      "Function.create({ name: 'Bar', body: 'x => foo.zzz(x)' })") shouldBe (
      """|error: Invalid database schema update.
         |    error: Type `fooCollection` does not have field `zzz`
         |    at main.fsl:9:7
         |      |
         |    9 |   foo.zzz(x)
         |      |       ^^^
         |      |
         |    hint: Type `fooCollection` inferred here
         |    at main.fsl:5:12
         |      |
         |    5 | collection foo {
         |      |            ^^^
         |      |
         |at *query*:1:16
         |  |
         |1 | Function.create({ name: 'Bar', body: 'x => foo.zzz(x)' })
         |  |                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
         |  |""".stripMargin
    )
  }
}
