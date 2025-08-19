/*
 * Copyright 2025 GAIM-TECH-OM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.killbill.billing.plugin.ompay;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;

import org.jooby.Request;
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

@Singleton
@Path("/form")
public class OmPayFormServlet extends PluginServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(OmPayFormServlet.class);
    private final transient OmPayPaymentPluginApi paymentPluginApi;
    private final transient OSGIKillbillAPI killbillAPI;
    private final transient OSGIKillbillClock clock;

    @Inject
    public OmPayFormServlet(final OmPayPaymentPluginApi paymentPluginApi,
                            final OSGIKillbillAPI killbillAPI,
                            final OSGIKillbillClock clock) {
        this.paymentPluginApi = paymentPluginApi;
        this.killbillAPI = killbillAPI;
        this.clock = clock;
    }

    @GET
    public Result getFormDescriptor(final Request req,
                                    @Local @Named("killbill_tenant") final Tenant tenant) {
        try {
            // Extract query parameters for the form fields
            String kbAccountIdStr = req.param("kbAccountId").value(); // Throws Err.Missing if not present
            String returnUrl = req.param("returnUrl").value();
            String cancelUrl = req.param("cancelUrl").value();
            String paymentIntent = req.param("paymentIntent").toOptional().orElse("auth");
            String currency = req.param("currency").toOptional().orElse("USD");
            String amount = req.param("amount").toOptional().orElse("0");

            // Validate required parameters
            if (kbAccountIdStr == null || returnUrl == null || cancelUrl == null) {
                return Results.with("Missing required query parameters: kbAccountId, returnUrl, cancelUrl").status(Status.BAD_REQUEST);
            }
            UUID kbAccountId = UUID.fromString(kbAccountIdStr);

            logger.info("Building form descriptor for account {}: amount={}, currency={}, paymentIntent={}",
                    kbAccountId, amount, currency, paymentIntent);

            final CallContext context = new PluginCallContext(OmPayActivator.PLUGIN_NAME,
                    clock.getClock().getUTCNow(), kbAccountId, tenant.getId());

            // Build custom field properties with the parameters
            List<PluginProperty> customFields = new ArrayList<>();

            if (!Strings.isNullOrEmpty(amount)) {
                customFields.add(new PluginProperty("amount", amount, false));
            }

            if (!Strings.isNullOrEmpty(currency)) {
                customFields.add(new PluginProperty("currency", currency, false));
            }

            // Default to "sale" if not provided for payment intent
            if (!Strings.isNullOrEmpty(paymentIntent)) {
                customFields.add(new PluginProperty("paymentIntent", paymentIntent, false));
            } else {
                customFields.add(new PluginProperty("paymentIntent", "sale", false));
            }

            if (!Strings.isNullOrEmpty(returnUrl)) {
                customFields.add(new PluginProperty("returnUrl", returnUrl, false));
            }

            if (!Strings.isNullOrEmpty(cancelUrl)) {
                customFields.add(new PluginProperty("cancelUrl", cancelUrl, false));
            }

            // Call the plugin API to build the form descriptor
            HostedPaymentPageFormDescriptor descriptor = paymentPluginApi.buildFormDescriptor(
                    kbAccountId,
                    customFields,
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