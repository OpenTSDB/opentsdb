package net.opentsdb.utils;

import java.util.Properties;

public abstract class PluginConfig {

    protected String configUrl;
    private PluginConfig(){}
    protected Properties properties;

    public PluginConfig(String configUrl){
        this.configUrl = configUrl;
    }

    public  Properties getConfig() {
        return properties;
    }

}
