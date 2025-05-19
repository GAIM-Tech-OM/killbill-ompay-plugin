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
import org.jooby.mvc.Local;
import org.jooby.mvc.Path;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillClock;
import org.killbill.billing.payment.api.PluginProperty;
import org.killbill.billing.payment.plugin.api.HostedPaymentPageFormDescriptor;
import org.killbill.billing.payment.plugin.api.PaymentPluginApiException;
import org.killbill.billing.plugin.api.PluginCallContext;
import org.killbill.billing.plugin.core.PluginServlet;
import org.killbill.billing.tenant.api.Tenant;
import org.killbill.billing.util.callcontext.CallContext;
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
                                    @Named("kbAccountId") final UUID kbAccountId,
                                    @Local @Named("killbill_tenant") final Tenant tenant) {
        try {
            final CallContext context = new PluginCallContext(OmPayActivator.PLUGIN_NAME,
                    clock.getClock().getUTCNow(), kbAccountId, tenant.getId());

            // Call the plugin API to build the form descriptor
            HostedPaymentPageFormDescriptor descriptor = paymentPluginApi.buildFormDescriptor(
                    kbAccountId,
                    null,
                    null,
                    context
            );

            return Results.with(descriptor, Status.CREATED).type("application/json");

        } catch (PaymentPluginApiException e) {
            logger.error("Error building form descriptor", e);
            return Results.with("Error: " + e.getMessage(), Status.SERVER_ERROR)
                    .header("Content-Type", "text/plain");
        }
    }
}