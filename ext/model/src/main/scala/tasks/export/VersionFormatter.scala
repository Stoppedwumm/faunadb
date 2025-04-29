package fauna.model.tasks.export

import fauna.atoms._
import fauna.auth.{ AdminPermissions, EvalAuth }
import fauna.codex.json2.JSONWriter
import fauna.lang.syntax._
import fauna.model.runtime.fql2.{ FQLInterpreter, ReadBroker, Result }
import fauna.model.runtime.fql2.serialization.{
  FQL2ValueEncoder,
  FQL2ValueMaterializer,
  ValueFormat
}
import fauna.model.schema.CollectionConfig
import fauna.model.Credentials
import fauna.repo.doc.Version
import fauna.repo.query.Query
import fauna.repo.values.Value
import io.netty.buffer.{ ByteBuf, ByteBufAllocator }

class VersionFormatter(
  collections: Map[CollectionID, CollectionConfig],
  format: ValueFormat) {

  def formatVersion(version: Version): Query[(DocID, ByteBuf)] = {
    val intp = new FQLInterpreter(EvalAuth(version.parentScopeID, AdminPermissions))

    val validTS = version.ts.validTS

    val fieldsQ =
      new ReadBroker(collections(version.collID))
        .getAllFields(
          intp,
          version,
          Some(validTS),
          evalComputedFields = false) mapT { struct =>
        // HACK: normally we do not expose hashed_password, but for
        // export, we do want to. There isn't a good way to do this by getting
        // an "internal" view of a CollectionConfig, so we break the
        // abstraction here.
        val hpw = Option
          .when(version.collID == CredentialsID.collID)(version)
          .flatMap {
            _.data(Credentials.HashedPasswordField).map { hpw =>
              val field = Seq("hashed_password" -> Value.Str(hpw))
              Value.Struct.fromSpecific(field)
            }
          }

        hpw.fold(struct)(struct ++ _)
      }

    val fmtQ = fieldsQ flatMapT { struct =>
      FQL2ValueMaterializer.materializeShallow(intp, struct) map { result =>
        val buf = ByteBufAllocator.DEFAULT.buffer

        FQL2ValueEncoder.encode(format, JSONWriter(buf), result, validTS)

        Result.Ok(version.id -> buf)
      }
    }

    fmtQ flatMap {
      case Result.Ok(value) => Query.value(value)
      case Result.Err(err) =>
        Query.fail(new IllegalStateException(err.failureMessage))
    }
  }
}
