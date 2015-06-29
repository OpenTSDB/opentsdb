
# Developing in IntelliJ

## Importing the project

1. Clone the project into a destination of your choice.

2. Copy the sample config files `config/logback.xml.sample` and `config/opentsdb.conf.sample`
   to `config/logback.xml` and `config/opentsdb.conf`.

3. Open the IntelliJ import wizard and select the project root folder. Follow the wizard
   instructions using the gradle import model.

4. The project relies on annotation processors for code generation so you need to enable these in
   IntelliJ. You usually enable this under the `Build, Execution, Deployment > Compiler > Annotation
   Processors` pane. Under this pane you also need to set "Store generated sources relative to" to
   "Module content root" and the production and test source directories to
   `build/generated-sources/main/java` and `build/generated-sources/test/java`.

5. At this point you should be able to build and run everything however please do read the caveats.

## Tools

The file `doc/intellij-opentsdb-style.xml` contains a pre-configured IntelliJ code style
configuration that you can copy to `.idea/codeStyleSettings.xml` for automatic formatting.

There is also a checkstyle configuration that can be used in IntelliJ if you install the plugin. The
checkstyle configuration is located in `checkstyle.xml`.

One final useful tool is the IntelliJ findbugs plugin which can help to spot unnecessary bugs. All
of the mentioned tools are generally executed as part of the build process and pull requests are
generally required to pass so a good habit to get into is to run all of them before committing any
changes.

## Caveats

Currently there is a problem with IntelliJ overwriting the generated test classes when it builds the
JMH source set so from time to time you may have to run the Gradle `testClasses` task to regenerate
them. See #156 for more details.

One other minor caveat is that some compile-only dependencies are included in the runtime for build
done in IntelliJ. See #155 for more details.