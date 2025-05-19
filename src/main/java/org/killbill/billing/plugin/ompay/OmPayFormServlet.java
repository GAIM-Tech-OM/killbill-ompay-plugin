package org.killbill.billing.plugin.ompay;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;

import org.jooby.Result;
import org.jooby.Results;
import org.jooby.Status;
import org.jooby.mvc.GET;
import org.jooby.mvc.Path;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillClock;
import org.killbill.billing.payment.api.PluginProperty;
import org.killbill.billing.payment.plugin.api.HostedPaymentPageFormDescriptor;
import org.killbill.billing.payment.plugin.api.PaymentPluginApiException;
import org.killbill.billing.plugin.api.PluginCallContext;
import org.killbill.billing.plugin.core.PluginServlet;
import org.killbill.billing.tenant.api.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

@Singleton
@Path("/form")
public class OmPayFormServlet extends PluginServlet {

    private static final Logger logger = LoggerFactory.getLogger(OmPayFormServlet.class);
    private final OmPayPaymentPluginApi paymentPluginApi;
    private final OSGIKillbillAPI killbillAPI;
    private final OSGIKillbillClock clock;

    @Inject
    public OmPayFormServlet(final OmPayPaymentPluginApi paymentPluginApi,
                            final OSGIKillbillAPI killbillAPI,
                            final OSGIKillbillClock clock) {
        this.paymentPluginApi = paymentPluginApi;
        this.killbillAPI = killbillAPI;
        this.clock = clock;
    }

    @GET
    public Result getFormDescriptor(final HttpServletRequest request,
                                    @Named("killbill_tenant") final Optional<Tenant> tenantOpt) {
        final String kbAccountIdStr = request.getParameter("kbAccountId");

        if (kbAccountIdStr == null || kbAccountIdStr.isEmpty()) {
            logger.warn("Missing required parameter: kbAccountId");
            return Results.with("Required parameter 'kbAccountId' missing", Status.BAD_REQUEST)
                    .header("Content-Type", "text/plain");
        }

        try {
            UUID kbAccountId = UUID.fromString(kbAccountIdStr);

            // Optional parameters
            ImmutableList.Builder<PluginProperty> propertiesBuilder = ImmutableList.builder();

            // Add any parameters from the query string as plugin properties
            request.getParameterMap().forEach((key, values) -> {
                if (values != null && values.length > 0 && !key.equals("kbAccountId")) {
                    propertiesBuilder.add(new PluginProperty(key, values[0], false));
                }
            });

            final PluginCallContext callContext = new PluginCallContext(
                    OmPayActivator.PLUGIN_NAME,
                    clock.getClock().getUTCNow(),
                    UUID.randomUUID(),
                    tenantOpt.map(Tenant::getId).orElse(null)
            );

            // Call the plugin API to build the form descriptor
            HostedPaymentPageFormDescriptor descriptor = paymentPluginApi.buildFormDescriptor(
                    kbAccountId,
                    null,
                    propertiesBuilder.build(),
                    callContext
            );

            return Results.with(descriptor, Status.CREATED).type("application/json");

        } catch (IllegalArgumentException e) {
            logger.warn("Invalid UUID format for kbAccountId: {}", kbAccountIdStr);
            return Results.with("Invalid UUID format for kbAccountId", Status.BAD_REQUEST)
                    .header("Content-Type", "text/plain");
        } catch (PaymentPluginApiException e) {
            logger.error("Error building form descriptor", e);
            return Results.with("Error: " + e.getMessage(), Status.SERVER_ERROR)
                    .header("Content-Type", "text/plain");
        }
    }
}