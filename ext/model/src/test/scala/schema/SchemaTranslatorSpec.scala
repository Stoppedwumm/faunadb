package fauna.model.test

import fauna.auth.Auth
import fauna.model.schema.SchemaTranslator
import fql.ast.display._

class SchemaTranslatorSpec extends FQL2Spec {

  "SchemaTranslator" - {
    val auth = newDB
    val sAuth = Auth.adminForScope(auth.scopeID)

    "translates collections" in {
      evalOk(
        auth,
        """|Collection.create({
           |  name: 'C',
           |  history_days: 10,
           |  ttl_days: 691,
           |  indexes: {
           |    byName: {
           |      terms: [
           |        { field: "name" },
           |        { field: ".my_array[0]" },
           |        { field: ".my.nested.field[0]" },
           |        { field: ".['0'].foo" }
           |      ],
           |      values: [
           |        { field: "fame", order: "desc" },
           |        { field: "game", order: "asc", mva: true }
           |      ]
           |    },
           |    byFame: {
           |      terms: [{ field: "fame", mva: true }]
           |    }
           |  },
           |  constraints: [ { unique: ["name"] } ]
           |})""".stripMargin
      )
      val cols = ctx ! SchemaTranslator.translateCollections(auth.scopeID)
      cols.size shouldEqual 1
      cols.head.display shouldEqual
        """|collection C {
           |  index byName {
           |    terms [.name, .my_array[0], .my.nested.field[0], .["0"].foo]
           |    values [desc(.fame), mva(.game)]
           |  }
           |  index byFame {
           |    terms [mva(.fame)]
           |  }
           |  unique [.name]
           |  history_days 10
           |  ttl_days 691
           |}
           |""".stripMargin
    }

    "translates functions" in {
      evalOk(
        auth,
        "Function.create({ name: 'F', role: 'server', body: '(x, y) => x + y' })")
      evalOk(
        auth,
        "Function.create({ name: 'FF', alias: 'G', body: '(x, y) => x - y' })")
      evalOk(
        auth,
        """|Function.create({
           |  name: 'FFF',
           |  role: 'server',
           |  alias: 'H',
           |  body: 'x => x',
           |  signature: 'Number => Number'
           |})""".stripMargin
      )
      val fns = ctx ! SchemaTranslator.translateFunctions(auth.scopeID)
      fns.size shouldEqual 3

      fns.head.display shouldEqual
        """|@role(server)
           |function F(x, y) {
           |  x + y
           |}
           |""".stripMargin

      fns.init.last.display shouldEqual
        """|@alias(G)
           |function FF(x, y) {
           |  x - y
           |}
           |""".stripMargin

      fns.last.display shouldEqual
        """|@alias(H)
           |@role(server)
           |function FFF(x: Number): Number {
           |  x
           |}
           |""".stripMargin
    }

    "translates roles" in {
      evalOk(auth, "Collection.create({ name: 'Users' })")
      evalOk(auth, "Collection.create({ name: 'Orders' })")
      evalOk(auth, "Collection.create({ name: 'Products' })")

      evalOk(
        sAuth,
        """|Role.create({
           |  name: "AdminUsers",
           |  membership: [
           |    {
           |      resource: "Users",
           |      predicate: 'user => user.isAdmin'
           |    },
           |    {
           |      resource: "Orders"
           |    }
           |  ],
           |  privileges: [{
           |    resource: "Users",
           |    actions: {
           |      create: true,
           |      create_with_id: true,
           |      read: 'user => user.canRead'
           |    }
           |  },
           |  {
           |    resource: "Orders",
           |    actions: {
           |      read: 'order => order.foo'
           |    }
           |  },
           |  {
           |    resource: "Products",
           |    actions: {
           |      read: true,
           |      write: true
           |    }
           |  }]
           |})""".stripMargin
      )

      val roles = ctx ! SchemaTranslator.translateRoles(auth.scopeID)
      roles.size shouldEqual 1
      roles.head.display shouldEqual
        """|role AdminUsers {
           |  privileges Users {
           |    create
           |    create_with_id
           |    read {
           |      predicate ((user) => user.canRead)
           |    }
           |  }
           |  privileges Orders {
           |    read {
           |      predicate ((order) => order.foo)
           |    }
           |  }
           |  privileges Products {
           |    read
           |    write
           |  }
           |  membership Users {
           |    predicate ((user) => user.isAdmin)
           |  }
           |  membership Orders
           |}
           |""".stripMargin
    }

    "translates access providers" in {
      evalOk(sAuth, "Role.create({ name: 'MyRole' })")
      evalOk(sAuth, "Role.create({ name: 'YourRole' })")
      evalOk(
        sAuth,
        """|AccessProvider.create({
           |  name: "MyAP",
           |  issuer: "https://fauna0.auth0.com",
           |  jwks_uri: "https://fauna.auth0.com/.well-known/jwks.json",
           |  roles: [
           |    {
           |      role: "MyRole",
           |      predicate: 'arg => true'
           |    },
           |    "YourRole"
           |  ],
           |})
           |""".stripMargin
      )

      val aps = ctx ! SchemaTranslator.translateAPs(auth.scopeID)
      aps.size shouldEqual 1
      aps.head.display shouldEqual
        """|access provider MyAP {
           |  issuer "https://fauna0.auth0.com"
           |  jwks_uri "https://fauna.auth0.com/.well-known/jwks.json"
           |  role MyRole {
           |    predicate ((arg) => true)
           |  }
           |  role YourRole
           |}
           |""".stripMargin
    }
  }

  "translates defaults in nested fields" in {
    val auth = newDB

    evalOk(
      auth,
      """|Collection.create({
         |  name: "User",
         |  fields: {
         |    a: {
         |      signature: "Int"
         |    },
         |    address: {
         |      signature: "{ street: String = \"hello\" }"
         |    }
         |  }
         |})""".stripMargin
    )

    schemaContent(auth, "main.fsl") shouldBe Some(
      """|// The following schema is auto-generated.
         |// This file contains FSL for FQL10-compatible schema items.
         |
         |collection User {
         |  a: Int
         |  address: { street: String = "hello" }
         |}
         |""".stripMargin
    )
  }
}
