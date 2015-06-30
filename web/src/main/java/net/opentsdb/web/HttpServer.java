package net.opentsdb.web;

import net.opentsdb.core.ConfigModule;
import net.opentsdb.utils.EventLoopGroups;
import net.opentsdb.utils.InvalidConfigException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.File;

public final class HttpServer extends CommandLineApplication {
  public HttpServer(final OptionParser optionParser) {
    super("web", "[OPTIONS]", "Start the REST API server", optionParser);
  }

  /**
   * Entry-point for the http server application. The assign program is normally not executed
   * directly but rather through the main project.
   *
   * @param args The command-line arguments
   */
  public static void main(String[] args) {
    final OptionParser optionParser = new OptionParser();
    final HttpServer application = new HttpServer(optionParser);

    try {
      final OptionSet options = optionParser.parse(args);

      if (options.has("help")) {
        application.printHelp(optionParser);
      }

      configureLogger(options.valueOf(application.getLoggerConfigSpec()));

      final File configFile = options.valueOf(application.getConfigSpec());

      HttpServerComponent httpServerComponent = DaggerHttpServerComponent.builder()
          .configModule(new ConfigModule(configFile))
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
      application.printError(e.getMessage());
      System.exit(42);
    } catch (InvalidConfigException | ConfigException e) {
      System.err.println(e.getMessage());
      System.exit(42);
    }
  }
}
