package net.opentsdb.tsd;

import dagger.Module;
import net.opentsdb.core.TsdbModule;

/**
 * This is the main dagger entrypoint for all tools.
 *
 * @see net.opentsdb.core.TsdbModule
 */
@Module(includes = TsdbModule.class,
    injects = {
        PipelineFactory.class
    })
public class ApiModule {
}
