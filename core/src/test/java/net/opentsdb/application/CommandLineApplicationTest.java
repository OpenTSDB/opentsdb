package net.opentsdb.application;

import static org.junit.Assert.assertSame;

import org.junit.Test;

public class CommandLineApplicationTest {
  @Test(expected = IllegalStateException.class)
  public void testBuilderThrowsOnEmptyCommand() throws Exception {
    CommandLineApplication.builder()
        .command("")
        .description("description")
        .helpText("helpText")
        .usage("usage")
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderThrowsOnEmptyDescription() throws Exception {
    CommandLineApplication.builder()
        .command("command")
        .description("")
        .helpText("helpText")
        .usage("usage")
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderThrowsOnEmptyHelpText() throws Exception {
    CommandLineApplication.builder()
        .command("command")
        .description("description")
        .helpText("")
        .usage("usage")
        .build();
  }

  @Test(expected = IllegalStateException.class)
  public void testBuilderThrowsOnEmptyUsage() throws Exception {
    CommandLineApplication.builder()
        .command("command")
        .description("description")
        .helpText("helpText")
        .usage("")
        .build();
  }

  @Test
  public void testBuilderOutputStreamIsSystemErrByDefault() throws Exception {
    final CommandLineApplication application = CommandLineApplication.builder()
        .command("command")
        .description("description")
        .helpText("helpText")
        .usage("usage")
        .build();

    assertSame(System.err, application.outputStream());
  }
}