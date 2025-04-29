package fauna

import scala.concurrent.Future

package object lang {
  object syntax
      extends fauna.lang.ArraySyntax
      with fauna.lang.ArraySeqSyntax
      with fauna.lang.ByteBufSyntax
      with fauna.lang.ByteBufferSyntax
      with fauna.lang.ChannelSyntax
      with fauna.lang.ChannelFutureSyntax
      with fauna.lang.DurationSyntax
      with fauna.lang.FileChannelSyntax
      with fauna.lang.FileSyntax
      with fauna.lang.FutureSyntax
      with fauna.lang.LoggingSyntax
      with fauna.lang.LongSyntax
      with fauna.lang.MonadSyntax
      with fauna.lang.MonadTSyntax
      with fauna.lang.PageSyntax
      with fauna.lang.PathSyntax
      with fauna.lang.RandomSyntax
      with fauna.lang.ReferenceCountedSyntax
      with fauna.lang.ReleasableSyntax
      with fauna.lang.SeqSyntax
      with fauna.lang.OptionSyntax
      with fauna.lang.StringSyntax {

    object array extends fauna.lang.ArraySyntax
    object arrayseq extends fauna.lang.ArraySeqSyntax
    object buf extends fauna.lang.ByteBufSyntax
    object buffer extends fauna.lang.ByteBufferSyntax
    object channel extends fauna.lang.ChannelSyntax
    object channelfuture extends fauna.lang.ChannelFutureSyntax
    object duration extends fauna.lang.DurationSyntax
    object file extends fauna.lang.FileSyntax
    object filechannel extends fauna.lang.FileChannelSyntax
    object future extends fauna.lang.FutureSyntax
    object logging extends fauna.lang.LoggingSyntax
    object long extends fauna.lang.LongSyntax
    object monad extends fauna.lang.MonadSyntax
    object monadt extends fauna.lang.MonadTSyntax
    object page extends fauna.lang.PageSyntax
    object path extends fauna.lang.PathSyntax
    object random extends fauna.lang.RandomSyntax
    object refcounted extends fauna.lang.ReferenceCountedSyntax
    object releasable extends fauna.lang.ReleasableSyntax
    object seq extends fauna.lang.SeqSyntax
    object option extends fauna.lang.OptionSyntax
    object string extends fauna.lang.StringSyntax
  }

  type IsMonad[MA] = Unpack[Monad, MA]

  type IsMonadDistribute[MA] = Unpack[MonadDistribute, MA]

  type IsMonadStack[OMA] = Unpack2[Monad, MonadDistribute, OMA]

  type IsMonadEmptyDistribute[MA] = Unpack[MonadEmptyDistribute, MA]

  type IsMonadAltDistribute[MA] = Unpack[MonadAltDistribute, MA]

  type PagedFuture[+A] = Future[Page[Future, A]]
}
