package fauna.prop.api

import fauna.codex.json._
import fauna.lang.clocks.Clock
import fauna.limits.AccountLimits
import fauna.net.security.JWK
import fauna.prop._
import fauna.prop.api.DefaultQueryHelpers._

trait APIGenerators extends JSGenerators with APIResponseHelpers with ResponseCodes {
  def client: FaunaDB.Client

  val anAction = Prop.boolean map { if (_) "create" else "delete" }

  val aTimestamp: Prop[JSLong] = Prop.long(Clock.millis) map { JSLong(_) }

  val aSchemaName = for {
    name <- aName
    if !Set("events", "self", "_").contains(name)
  } yield name

  val aUniqueSchemaName = for {
    name <- aUniqueName
    if !Set("events", "self", "_").contains(name)
  } yield name

  val aUniqueDBName = for {
    name <- aUniqueSchemaName
    if client.api.get(s"/databases/$name", FaunaDB.rootKey).statusCode == NotFound
  } yield name

  def aDatabase(apiVers: String): Prop[Database] =
    aUniqueDBName map { FaunaDB.makeDB(_, client.api, apiVers) }

  def aDatabase(apiVers: String, parent: Database): Prop[Database] =
    aUniqueDBName map { FaunaDB.makeDB(_, client.api, apiVers, parent.adminKey) }

  def aContainerDB(apiVers: String, parent: Database): Prop[Database] =
    aUniqueDBName map {
      FaunaDB.makeDB(_, client.api, apiVers, parent.adminKey, container = true)
    }

  def aContainerDB(apiVers: String, accountID: Long): Prop[Database] =
    aUniqueDBName map {
      FaunaDB.makeDB(_, client.api, apiVers, FaunaDB.rootKey, container = true, accountID = Some(accountID))
    }

  def aContainerDB(apiVers: String, parent: Database, accountID: Long): Prop[Database] =
    aUniqueDBName map {
      FaunaDB.makeDB(_, client.api, apiVers, parent.adminKey, container = true, accountID = Some(accountID))
    }

  def aContainerDB(
    apiVers: String,
    parent: Database,
    accountID: Long,
    limits: AccountLimits): Prop[Database] =
    aUniqueDBName map {
      FaunaDB.makeDB(
        _,
        client.api,
        apiVers,
        parent.adminKey,
        container = true,
        accountID = Some(accountID),
        limits = Some(limits))
    }

  def aUserOfCollection(
    db: Database,
    collection: Collection
  ): Prop[User] =
    for {
      inst  <- aDocument(collection)
      pass  <- Prop.string(1, 40)
      email <- Prop.anEmailAddress
      cred <- mkCredentials(db, inst, pass)
    } yield {
      val token =
        client. api.query(Login(inst.refObj, MkObject("password" -> pass)), db.clientKey)
      User(collection, inst, pass, email, token.resource, cred, db)
    }

  def aUser(db: Database): Prop[User] =
    aCollection(db) flatMap { aUserOfCollection(db, _) }

  val aKeyRole = Prop.choose(Seq("admin", "server", "server-readonly", "client"))

  def aKey(
    auth: Database,
    database: Option[Database] = None,
    roleProp: Prop[String] = aKeyRole
  ): Prop[Key] =
    for {
      role <- roleProp
      res = client.api.query(
        CreateKey(
          MkObject(
            "role" -> role,
            "database" -> database.map { _.refObj }
          )
        ),
        auth.adminKey
      )
      if res.statusCode == Created
    } yield {
      Key(res.resource)
    }

  def aKey(role: Role): Prop[Key] =
    for {
      res <- Prop { _ =>
        client.api.query(
          CreateKey(MkObject("role" -> role.refObj)),
          role.database.adminKey
        )
      }
      if res.statusCode == Created
    } yield {
      Key(res.resource)
    }

  def aJWT(db: Database, ref: JSValue, expireIn: JSValue = JSNull): Prop[JWTToken] = {
    for {
      res <- Prop { _ =>
        client.api.query(
          if (expireIn != JSNull) IssueAccessJWT(ref, expireIn) else IssueAccessJWT(ref),
          db.adminKey
        )
      }
      if res.statusCode == OK
    } yield {
      JWTToken(res.resource)
    }
  }

  def aJWK =
    for {
      keyPair <- Prop.aRSAKeyPair()
      kid <- Prop.hexString()
    } yield {
      JWK.rsa(kid, keyPair)
    }

  def aFaunaClass(
    database: Database,
    nameProp: Prop[String] = aSchemaName
  ): Prop[Collection] = aCollection(database, nameProp)

  def aCollection(
    database: Database,
    nameProp: Prop[String] = aSchemaName,
    ttlDays: Option[Long] = None,
    historyDays: Option[Long] = None
  ): Prop[Collection] =
    for {
      name <- nameProp
      data <- jsObject
      chk = client.api.get(s"/classes/$name", database.key)
      if chk.statusCode == NotFound
      res = client.api.query(
        CreateClass(
          MkObject(
            "name" -> name,
            "ttl_days" -> ttlDays,
            "history_days" -> historyDays,
            "data" -> Quote(data)
          )
        ),
        database.key
      )
      if res.statusCode == Created
    } yield {
      Collection(res.resource, database)
    }

  def anIndex(
    source: Collection,
    termProp: Prop[Seq[JSValue]] = Prop.const(Seq(JSArray("data", "foo"))),
    valueProp: Prop[Seq[(JSValue, Boolean)]] = Prop.const(Nil),
    partitionsProp: Prop[Option[Int]] = Prop.const(None),
    uniqueProp: Prop[Boolean] = Prop.const(false),
    nameProp: Prop[String] = aSchemaName
  ) =
    for {
      name <- nameProp
      terms <- termProp map {
        _ map { s =>
          MkObject("field" -> s)
        }
      }
      values <- valueProp map {
        _ map { case (s, r) => MkObject("field" -> s, "reverse" -> r) }
      }
      partitions <- partitionsProp
      unique <- uniqueProp
      chk = client.api.get(s"/indexes/$name", source.database.key)
      if chk.statusCode == NotFound

      res = client.api.query(
        CreateIndex(
          MkObject(
            "name" -> name,
            "source" -> source.refObj,
            "terms" -> terms,
            "values" -> values,
            "active" -> true,
            "partitions" -> partitions,
            "unique" -> unique
          )
        ),
        source.database.key
      )
      if res.statusCode == Created
    } yield {
      Index(res.resource, source)
    }

  @annotation.nowarn("msg=parameter res*") // XXX: compiler bug?
  def aMatchSet(db: Database, instCount: Prop[Int] = Prop.int(1 to 5)) =
    for {
      cls <- aCollection(db)
      idx <- anIndex(
        cls,
        termProp = Prop.const(Seq(JSArray("data", "foo"))),
        valueProp =
          Prop.const(Seq((JSArray("data", "bar"), false), (JSArray("ref"), false)))
      )
      count <- instCount
      res <- Prop { _ =>
        val q = CreateF(
          cls.refObj,
          MkObject("data" -> MkObject("foo" -> 1, "bar" -> NewID))
        )
        client.api.query(q, db.key)
      } * count
      if res forall { _.statusCode == Created }
    } yield (cls, idx, count, Match(idx.refObj, 1))

  def someDocuments(
    count: Int,
    collection: Collection,
    dataProp: Prop[JSObject] = jsObject
  ): Prop[Seq[JSObject]] =
    for {
      data <- dataProp * count
      res = client.api.query(
        JSArray(data map { d =>
          CreateF(
            collection.refObj,
            MkObject(
              "data" -> Quote(d)
            )
          )
        }: _*),
        collection.database.key
      )
      if Set(Created, OK) contains res.statusCode
    } yield {
      (res.json / "resource").as[Seq[JSObject]]
    }

  def aDocument(
    collection: Collection,
    dataProp: Prop[JSObject] = jsObject
  ): Prop[JSObject] =
    someDocuments(1, collection, dataProp) map { _.head }

  def anEvent(
    cls: Collection,
    ref: Option[JSObject] = None,
    ts: Prop[JSLong] = aTimestamp,
    action: Prop[String] = anAction,
    data: Prop[JSObject] = jsObject
  ): Prop[JSObject] = {
    val newID = JSObject("ref" -> cls.refObj, "id" -> NewID)

    for {
      t <- ts
      a <- action
      d <- data
      res = client.api.query(
        JSObject(
          "insert" -> ref.getOrElse(newID),
          "ts" -> t,
          "action" -> a,
          "params" -> MkObject("data" -> Quote(d))
        ),
        cls.database.key
      )
      if res.statusCode == OK
    } yield {
      res.resource
    }
  }

  def aRole(db: Database, privileges: Privilege*): Prop[Role] =
    aRole(db, Seq.empty, privileges: _*)

  def aRole(
    db: Database,
    membership: Membership,
    privileges: Privilege*
  ): Prop[Role] = aRole(db, Seq(membership), privileges: _*)

  def aRole(
    db: Database,
    memberships: Seq[Membership],
    privileges: Privilege*
  ): Prop[Role] =
    aRole(db, aUniqueSchemaName, memberships, privileges)

  def aRole(
    db: Database,
    name: Prop[String],
    memberships: Seq[Membership] = Seq.empty,
    privileges: Seq[Privilege] = Seq.empty
  ): Prop[Role] = {
    for {
      name <- name
      res = client.api.query(
        CreateRole(
          MkObject(
            "name" -> name,
            "membership" -> JSArray(memberships map { membership =>
              MkObject(
                "resource" -> membership.resource.refObj,
                "predicate" -> membership.predicate
              )
            }: _*),
            "privileges" -> JSArray(privileges map { privilege =>
              MkObject(
                "resource" -> privilege.resource,
                "actions" -> MkObject(
                  "read" -> privilege.read.config,
                  "write" -> privilege.write.config,
                  "create" -> privilege.create.config,
                  "create_with_id" -> privilege.createWithID.config,
                  "delete" -> privilege.delete.config,
                  "history_write" -> privilege.historyWrite.config,
                  "history_read" -> privilege.historyRead.config,
                  "unrestricted_read" -> privilege.unrestrictedRead.config,
                  "call" -> privilege.call.config
                )
              )
            }: _*)
          )
        ),
        db.adminKey
      )
      if res.statusCode == Created
    } yield {
      Role(res.resource, db, name)
    }
  }

  def anAccessProvider(
    db: Database,
    issuer: Prop[String] = Prop.alphaString(),
    jwksUrl: Prop[String] = Prop.aJwksUri,
    roles: Seq[AccessProviderRole] = Seq.empty): Prop[AccessProvider] = {
    for {
      name <- aUniqueSchemaName
      iss  <- issuer
      url  <- jwksUrl
      res = client.api.query(
        CreateAccessProvider(
          MkObject(
            "name" -> name,
            "issuer" -> iss,
            "jwks_uri" -> url,
            "roles" -> JSArray(roles map {
              case AccessProviderRole(role, Some(predicate)) =>
                MkObject("role" -> role.refObj, "predicate" -> predicate)

              case AccessProviderRole(role, None) =>
                role.refObj
            }: _*)
          )),
        db.adminKey
      )
      if res.statusCode == Created
    } yield {
      AccessProvider(res.resource, name, db)
    }
  }

  def aFunc(
    db: Database,
    body: JSValue = Lambda("x" -> Var("x")),
    role: String = null
  ): Prop[UserFunction] = {

    val bdy = body match {
      case _: JSString => body
      case _           => QueryF(body)
    }

    for {
      name <- aSchemaName
      res = client.api.query(
        CreateFunction(
          MkObject(
            "name" -> name,
            "body" -> bdy,
            "role" -> Option(role)
          )
        ),
        db.key
      )
      if res.statusCode == Created
    } yield {
      UserFunction(res.resource, name)
    }
  }

  def mkCredentials(
    db: Database,
    owner: JSObject,
    password: String
  ): Prop[JSObject] =
    for {
      data <- jsObject
      res = client.api.query(
        CreateF(
          Ref("credentials"),
          MkObject(
            "instance" -> owner.refObj,
            "password" -> password,
            "data" -> Quote(data)
          )
        ),
        db.key
      )
      if res.statusCode == Created
    } yield res.resource
}
