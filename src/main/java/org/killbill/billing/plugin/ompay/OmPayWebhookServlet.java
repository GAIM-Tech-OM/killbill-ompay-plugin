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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;

import org.jooby.Result;
import org.jooby.Results;
import org.jooby.Status;
import org.jooby.mvc.Body;
import org.jooby.mvc.POST;
import org.jooby.mvc.Path;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillClock;
import org.killbill.billing.payment.plugin.api.PaymentPluginApiException;
import org.killbill.billing.plugin.api.PluginCallContext;
import org.killbill.billing.plugin.core.PluginServlet;
import org.killbill.billing.tenant.api.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Singleton
@Path("/webhook")
public class OmPayWebhookServlet extends PluginServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(OmPayWebhookServlet.class);
    private final transient OmPayPaymentPluginApi paymentPluginApi;
    private final transient OmPayConfigurationHandler configurationHandler;
    private final transient OSGIKillbillAPI killbillAPI;
    private final transient OSGIKillbillClock clock;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public OmPayWebhookServlet(final OmPayPaymentPluginApi paymentPluginApi,
                               final OmPayConfigurationHandler configurationHandler,
                               final OSGIKillbillAPI killbillAPI,
                               final OSGIKillbillClock clock) {
        this.paymentPluginApi = paymentPluginApi;
        this.configurationHandler = configurationHandler;
        this.killbillAPI = killbillAPI;
        this.clock = clock;
    }

    @POST
    public Result handleWebhook(final HttpServletRequest request,
                                @Named("killbill_tenant") final Optional<Tenant> tenantOpt,
                                @Body final String body) {
        final PluginCallContext context = createPluginCallContext("webhook", tenantOpt.orElse(null), request);
        logger.info("Received OMPay webhook notification");

        if (body == null || body.isEmpty()) {
            logger.warn("Received empty webhook body");
            return Results.with("Empty notification body", Status.BAD_REQUEST);
        }

        try {
            // Validate webhook signature if provided
            if (!validateWebhookSignature(request, body, tenantOpt.orElse(null))) {
                logger.warn("Invalid webhook signature");
                return Results.with("Invalid webhook signature", Status.UNAUTHORIZED);
            }

            // Parse JSON to validate it's well-formed
            JsonNode rootNode = objectMapper.readTree(body);

            // Log webhook event details
            String resourceType = rootNode.path("resource_type").asText("unknown");
            String eventKind = rootNode.path("kind").asText("unknown");
            String notificationId = rootNode.path("id").asText("unknown");

            logger.info("Processing webhook: id={}, type={}, kind={}", notificationId, resourceType, eventKind);

            // Process the notification
            paymentPluginApi.processNotification(body, null, context);

            // Return successful response
            return Results.with("Webhook processed successfully", Status.OK);

        } catch (JsonProcessingException e) {
            logger.error("Error parsing webhook JSON: {}", body, e);
            return Results.with("Invalid JSON format: " + e.getMessage(), Status.BAD_REQUEST);
        } catch (PaymentPluginApiException e) {
            logger.error("Error processing webhook: {}", e.getMessage(), e);
            return Results.with("Error processing webhook: " + e.getMessage(), Status.SERVER_ERROR);
        } catch (Exception e) {
            logger.error("Unexpected error processing webhook: {}", e.getMessage(), e);
            return Results.with("Internal server error", Status.SERVER_ERROR);
        }
    }

    private boolean validateWebhookSignature(HttpServletRequest request, String body, Tenant tenant) {
        if (tenant == null) {
            logger.warn("Cannot validate webhook signature: tenant not found");
            return true;
        }


        String signature = request.getHeader("X-OMPay-Signature");
        if (signature == null || signature.isEmpty()) {
            logger.debug("No signature header found, skipping signature validation");
            return true;
        }

        // TODO: Implement actual signature validation once OMPay provides signature details
        // This would typically involve:
        // 1. Getting the shared secret from config (webhook signing key)
        // 2. Creating a HMAC signature using the body and comparing with the provided signature

        return true; // Placeholder for actual implementation
    }

    private PluginCallContext createPluginCallContext(final String apiName, final Tenant tenant, final HttpServletRequest request) {
        final UUID tenantId = (tenant != null) ? tenant.getId() : null;
        return new PluginCallContext(OmPayActivator.PLUGIN_NAME, clock.getClock().getUTCNow(), UUID.randomUUID(), tenantId);
    }

    private String readRequestBody(HttpServletRequest request) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;
        try (BufferedReader reader = request.getReader()) {
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }
}