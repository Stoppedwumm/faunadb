package fauna.qa.generators.test

import fauna.codex.json._
import fauna.lang.Timestamp
import fauna.prop.Prop
import fauna.prop.api._
import fauna.qa._

/**
  * A version of the bank transfer test from Jepsen. Some number of
  * accounts are created with an initial balance. The test will generate
  * transactions meant to move a random amount from one account to another.
  * No account should ever go negative and no account should ever have
  * more than the total amount the system started with.
  *  - bank.num-accounts = number of accounts in the system
  *  - bank.starting-balance = initial balance of each account
  *  - bank.max-transfer = maximum amount of each transfer
  */
class BankGenerator(name: String, fConfig: QAConfig)
    extends TestGenerator(name, fConfig)
    with JSGenerators
    with ValidatingTestGenerator {

  override val expectFailures = true

  val numAccounts = fConfig.getInt("bank.num-accounts")
  val startingBalance = fConfig.getInt("bank.starting-balance")
  val maxTransfer = fConfig.getInt("bank.max-transfer")

  val accounts = (1 to numAccounts) map { n =>
    Ref(s"classes/accounts/$n")
  }
  val accountP = Prop.choose(accounts)

  val amountP = Prop.int(1 to maxTransfer)

  def transfer(auth: String) = {
    val a, b = accountP.sample
    val amount = amountP.sample
    FaunaQuery(
      authKey = auth,
      maxContentionRetries = Some(1),
      query = Do(
        Let("a" -> Subtract(Select(JSArray("data", "balance"), Get(a)), amount)) {
          If(
            Or(LessThan(Var("a"), 0)),
            Abort("balance would go negative"),
            Update(a, MkObject("data" -> MkObject("balance" -> Var("a"))))
          )
        },
        Let("b" -> AddF(Select(JSArray("data", "balance"), Get(b)), amount)) {
          Update(b, MkObject("data" -> MkObject("balance" -> Var("b"))))
        }
      )
    )
  }

  val schema = Schema.DB(
    "bank",
    // NB. validation queries use history, so we must maintain it.
    Schema.Collection("accounts", historyDays = Some(30)))

  override protected def initializer(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestIterator("init", accounts map { acc =>
      FaunaQuery(
        auth,
        CreateF(acc, MkObject("data" -> MkObject("balance" -> startingBalance)))
      )
    })
  }

  def stream(schema: Schema.DB) = {
    val auth = schema.serverKey.get
    RequestStream("transfer", () => transfer(auth))
  }

  override def getValidateStateQuery(schema: Schema.DB, ts: Timestamp): FaunaQuery = {
    val auth = schema.serverKey.get
    FaunaQuery(
      auth,
      At(
        // Must run query at the given timestamp to ensure temporality of queries across nodes
        Time(ts.toString()),
        Let(
          "per_acct_bal" -> MapF(
            Lambda(
              "x" -> Seq(
                Select(Seq("ref", "id"), Get(Var("x"))),
                Select(Seq("data", "balance"), Get(Var("x"))))
            ),
            Paginate(Documents(ClassRef("accounts")))
          )
        ) {
          Seq(
            Var("per_acct_bal"),
            If(
              // Testing Bank invariant that sum of all account balances must be equal
              // to the starting total balance.
              Not(Equals(
                numAccounts * startingBalance,
                Select(
                  0,
                  Sum(
                    MapF(
                      Lambda(
                        "bal" -> Select(1, Var("bal"))
                      ),
                      Var("per_acct_bal")
                    )
                  )
                )
              )),
              // If invariant is violated, Abort with "account:balance" pairs for debugging
              Abort(
                Concat(
                  Prepend(
                    JSString("Total balance doesn't equal expected balance:"),
                    Select("data",
                      MapF(
                        Lambda(
                          "id_and_balance" ->
                          Let(
                            "id" -> Select(0, Var("id_and_balance")),
                            "balance" -> Select(1, Var("id_and_balance"))
                          ) {
                            Concat(
                              Seq(
                                Var("id"),
                                ToString(Var("balance"))),
                              ":")
                          }
                        ),
                        Var("per_acct_bal")
                      )
                    )
                  ),
                  " "
                )
              ),
              true
            )
          )
        }
      )
    )
  }
}

class JepsenBank(config: QAConfig) extends QATest {

  val generators: Array[TestGenerator] = Array(
    new BankGenerator("bank-transfers", config)
  )
}
