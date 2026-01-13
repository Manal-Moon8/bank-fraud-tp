package tp1

object TP1Functional {

  // ===== Modèles =====
  final case class Transaction(
    id: Int,
    userId: Int,
    amount: Double,
    country: String,
    status: String
  )

  final case class User(
    id: Int,
    name: String,
    age: Option[Int],
    active: Boolean
  )

  // ===== Données (exemple imparfait) =====
  private val users: List[User] = List(
    User(1, "Alice",   Some(24), true),
    User(2, "Bob",     None,     true),
    User(3, "Charlie", Some(17), false),
    User(4, "Diana",   Some(45), true),
    User(5, "Eve",     Some(30), false)
  )

  private val transactions: List[Transaction] = List(
    Transaction(1, 1, 120.0, "FR", "VALID"),
    Transaction(2, 1, -50.0, "FR", "VALID"),
    Transaction(3, 2, 200.0, "FR", "VALID"),
    Transaction(4, 3, 80.0,  "US", "CANCELLED"),
    Transaction(5, 4, 300.0, "FR", "VALID"),
    Transaction(6, 5, 150.0, "FR", "VALID"),
    Transaction(7, 99, 500.0, "FR", "VALID")
  )

  // 1) Utilisateur éligible : actif + majeur (age défini et >= 18)
  def isEligibleUser(user: User): Boolean =
    user.active && user.age.exists(_ >= 18)

  // 2) Recherche utilisateur (Option)
  def findUser(userId: Int): Option[User] =
    users.find(_.id == userId)

  // 3) Transaction valide : montant > 0, FR, VALID
  def isValidTransaction(t: Transaction): Boolean =
    t.amount > 0 && t.country == "FR" && t.status == "VALID"

  // 4) Pipeline final : montants éligibles
  def eligibleAmounts: List[Double] =
    transactions
      .filter(isValidTransaction)
      .flatMap(t => findUser(t.userId).filter(isEligibleUser).map(_ => t.amount))

  def totalEligibleAmount: Double =
    eligibleAmounts.sum

  // Bonus : moyenne Option[Double]
  def averageEligibleAmount: Option[Double] = {
    val xs = eligibleAmounts
    if (xs.isEmpty) None else Some(xs.sum / xs.size)
  }

  def main(args: Array[String]): Unit = {
    val total = totalEligibleAmount
    val avg   = averageEligibleAmount

    println(s"Montant total des transactions éligibles = $total")
    println(s"Montant moyen des transactions éligibles = ${avg.getOrElse("N/A")}")
  }
}
