package net.opentsdb.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;

import java.util.concurrent.ThreadFactory;

public class EventLoopGroups {
  private static EventLoopGroup bossGroup;
  private static EventLoopGroup workerGroup;

  public static synchronized EventLoopGroup sharedBossGroup(int hintedParallelism) {
    if (bossGroup == null) {
      bossGroup = new EpollEventLoopGroup(hintedParallelism, threadFactory("boss"));
    }

    return bossGroup;
  }

  public static synchronized EventLoopGroup sharedWorkerGroup(int hintedParallelism) {
    if (workerGroup == null) {
      workerGroup = new EpollEventLoopGroup(hintedParallelism, threadFactory("worker"));
    }

    return workerGroup;
  }

  private static ThreadFactory threadFactory(final String bossOrWorker) {
    return new ThreadFactoryBuilder()
        .setNameFormat(bossOrWorker + "-%d")
        .setDaemon(false)
        .setPriority(Thread.NORM_PRIORITY)
        .build();
  }
}
