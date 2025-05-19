package org.killbill.billing.plugin.ompay;

import java.util.Hashtable;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import org.killbill.billing.osgi.api.Healthcheck;
import org.killbill.billing.osgi.api.OSGIPluginProperties;
import org.killbill.billing.osgi.libs.killbill.KillbillActivatorBase;
import org.killbill.billing.payment.plugin.api.PaymentPluginApi;
import org.killbill.billing.plugin.core.config.PluginEnvironmentConfig;
import org.killbill.billing.plugin.core.resources.jooby.PluginApp;
import org.killbill.billing.plugin.core.resources.jooby.PluginAppBuilder;
import org.killbill.billing.plugin.ompay.client.OmPayHttpClient;
import org.killbill.billing.plugin.ompay.dao.OmPayDao;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OmPayActivator extends KillbillActivatorBase {

    private static final Logger logger = LoggerFactory.getLogger(OmPayActivator.class);
    public static final String PLUGIN_NAME = "killbill-ompay";

    private OmPayConfigurationHandler omPayConfigurationHandler;

    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);
        logger.info("Starting OmPay plugin activator");

        final String region = PluginEnvironmentConfig.getRegion(configProperties.getProperties());
        omPayConfigurationHandler = new OmPayConfigurationHandler(PLUGIN_NAME, killbillAPI, region);
        final OmPayConfigProperties globalConfiguration = omPayConfigurationHandler.createConfigurable(configProperties.getProperties());
        omPayConfigurationHandler.setDefaultConfigurable(globalConfiguration);

        final OmPayDao dao = new OmPayDao(dataSource.getDataSource());
        final OmPayHttpClient httpClient = new OmPayHttpClient();

        final PaymentPluginApi pluginApi = new OmPayPaymentPluginApi(
                omPayConfigurationHandler,
                killbillAPI,
                clock,
                dao);
        registerPaymentPluginApi(context, pluginApi);

        final Healthcheck healthcheck = new OmPayHealthcheck(omPayConfigurationHandler);
        registerHealthcheck(context, healthcheck);

        final PluginApp pluginApp = new PluginAppBuilder(PLUGIN_NAME,
                killbillAPI,
                dataSource,
                super.clock,
                configProperties)
                .withRouteClass(OmPayHealthcheckServlet.class)
                .withRouteClass(OmPayNonceHandlerServlet.class)
                .withRouteClass(OmPayWebhookServlet.class)
                .withService(pluginApi)
                .withService(clock)
                .withService(dao)
                .withService(omPayConfigurationHandler)
                .withService(httpClient)
                .withService(healthcheck)
                .build();

        final HttpServlet omPayServlet = PluginApp.createServlet(pluginApp);
        registerServlet(context, omPayServlet);
        logger.info("OmPay plugin activator started successfully");
    }

    @Override
    public void stop(final BundleContext context) throws Exception {
        logger.info("Stopping OmPay plugin activator");
        super.stop(context);
    }

    private void registerPaymentPluginApi(final BundleContext context, final PaymentPluginApi api) {
        final Hashtable<String, String> props = new Hashtable<>();
        props.put(OSGIPluginProperties.PLUGIN_NAME_PROP, PLUGIN_NAME);
        registrar.registerService(context, PaymentPluginApi.class, api, props);
    }

    private void registerHealthcheck(final BundleContext context, final Healthcheck healthcheck) {
        final Hashtable<String, String> props = new Hashtable<>();
        props.put(OSGIPluginProperties.PLUGIN_NAME_PROP, PLUGIN_NAME);
        registrar.registerService(context, Healthcheck.class, healthcheck, props);
    }

    private void registerServlet(final BundleContext context, final Servlet servlet) {
        final Hashtable<String, String> props = new Hashtable<>();
        props.put(OSGIPluginProperties.PLUGIN_NAME_PROP, PLUGIN_NAME);
        registrar.registerService(context, Servlet.class, servlet, props);
    }
}