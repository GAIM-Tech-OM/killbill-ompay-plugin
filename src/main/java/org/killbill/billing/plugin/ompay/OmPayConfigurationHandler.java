package org.killbill.billing.plugin.ompay;

import java.util.Properties;

import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.plugin.api.notification.PluginTenantConfigurableConfigurationHandler;

public class OmPayConfigurationHandler extends PluginTenantConfigurableConfigurationHandler<OmPayConfigProperties> {

    private final String region;

    public OmPayConfigurationHandler(final String pluginName,
                                                     final OSGIKillbillAPI osgiKillbillAPI,
                                                     final String region) {
        super(pluginName, osgiKillbillAPI);
        this.region = region;
    }

    @Override
    protected OmPayConfigProperties createConfigurable(final Properties properties) {
        return new OmPayConfigProperties(properties, region);
    }
}