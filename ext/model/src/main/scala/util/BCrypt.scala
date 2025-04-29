package fauna.util

import com.github.benmanes.caffeine.cache._
import com.github.benmanes.caffeine.cache.stats.CacheStats
import fauna.lang.syntax._
import fauna.stats.StatsRecorder
import java.util.concurrent.TimeUnit
import org.mindrot.jbcrypt.{ BCrypt => JBCrypt }

object BCrypt {
  case class Password(string: String, salt: String) {
    override def equals(other: Any) = other match {
      case o: Password => salt.secureEquals(o.salt) & string.secureEquals(o.string)
      case _                   => false
    }
  }

  val cacheLoader = new CacheLoader[Password, String] {
    def load(password: Password): String = JBCrypt.hashpw(password.string, password.salt)
  }

  val cache: LoadingCache[Password, String] = Caffeine.newBuilder
    .maximumSize(400000) // Approximately 50MB of bcrypt keys, I think
    .expireAfterAccess(5, TimeUnit.MINUTES)
    .recordStats()
    .build(cacheLoader)

  val DefaultRounds = 5

  @volatile private[this] var snapshot: CacheStats = cache.stats()

  def check(plaintext: String, hashed: String): Boolean =
    hashed.secureEquals(hash(plaintext, hashed))

  def hash(plaintext: String, salt: String): String = cache.get(Password(plaintext, salt))

  def hash(plaintext: String): String = hash(plaintext, JBCrypt.gensalt(DefaultRounds))

  def report(stats: StatsRecorder) = {
    val snap = cache.stats()
    val prev = snapshot
    snapshot = snap

    stats.count(s"Cache.BCrypt.Hits", (snap.hitCount - prev.hitCount).toInt)
    stats.count(s"Cache.BCrypt.Misses", (snap.missCount - prev.missCount).toInt)
    stats.count(s"Cache.BCrypt.Evictions", (snap.evictionCount - prev.evictionCount).toInt)
    stats.count(s"Cache.BCrypt.Loads", (snap.loadCount - prev.loadCount).toInt)
    stats.count(s"Cache.BCrypt.Objects", cache.asMap.size)
  }
}
