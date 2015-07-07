package net.opentsdb.web;

import net.opentsdb.application.CommandLineApplication;
import net.opentsdb.application.CommandLineOptions;
import net.opentsdb.utils.EventLoopGroups;
import net.opentsdb.utils.InvalidConfigException;

import com.google.common.io.Closeables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import joptsimple.OptionException;
import joptsimple.OptionSet;

public final class HttpServer {
  /**
   * Entry-point for the http server application. The assign program is normally not executed
   * directly but rather through the main project.
   *
   * @param args The command-line arguments
   */
  public static void main(String[] args) {
    // Release an FD that we don't need or want
    Closeables.closeQuietly(System.in);

    final CommandLineApplication cliApplication = CommandLineApplication.builder()
        .command("web")
        .description("Start the REST API server")
        .helpText("The REST server will respond to regular HTTP 1.1 requests")
        .usage("[OPTIONS]")
        .build();

    final CommandLineOptions cmdOptions = new CommandLineOptions();

    try {
      final OptionSet options = cmdOptions.parseOptions(args);

      if (options.has("help")) {
        cliApplication.printHelpAndExit(cmdOptions);
      }

      cmdOptions.configureLogger();

      HttpServerComponent httpServerComponent = DaggerHttpServerComponent.builder()
          .configModule(cmdOptions.configModule())
          .build();

      final Config config = httpServerComponent.config();

      final EventLoopGroup bossGroup = EventLoopGroups.sharedBossGroup(
          config.getInt("tsdb.web.threads.boss_group"));
      final EventLoopGroup workerGroup = EventLoopGroups.sharedWorkerGroup(
          config.getInt("tsdb.web.threads.worker_group"));

      try {
        final ServerBootstrap b = new ServerBootstrap()
            .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
            .option(ChannelOption.SO_BACKLOG, config.getInt("tsdb.web.backlog"))
            .option(ChannelOption.TCP_NODELAY, Boolean.TRUE)
            .option(ChannelOption.SO_KEEPALIVE, Boolean.TRUE)
            .option(ChannelOption.SO_REUSEADDR, Boolean.TRUE)
            .group(bossGroup, workerGroup)
            .channel(EpollServerSocketChannel.class)
            .handler(new LoggingHandler())
            .childHandler(httpServerComponent.httpServerInitializer());

        b.bind(config.getInt("tsdb.web.port")).sync().channel().closeFuture().sync();
      } finally {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
      }
    } catch (IllegalArgumentException | OptionException | InterruptedException e) {
      cliApplication.printError(e.getMessage());
      System.exit(42);
    } catch (InvalidConfigException | ConfigException e) {
      System.err.println(e.getMessage());
      System.exit(42);
    }
  }
}
