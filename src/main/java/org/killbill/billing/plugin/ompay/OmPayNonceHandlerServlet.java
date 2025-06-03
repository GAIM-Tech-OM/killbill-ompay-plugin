package org.killbill.billing.plugin.ompay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.jooby.*;
import org.jooby.mvc.Local;
import org.jooby.mvc.POST;
import org.jooby.mvc.Path;
import org.killbill.billing.account.api.Account;
import org.killbill.billing.account.api.AccountApiException;
import org.killbill.billing.catalog.api.Currency;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillClock;
import org.killbill.billing.payment.api.PaymentMethodPlugin;
import org.killbill.billing.payment.api.PluginProperty;
import org.killbill.billing.payment.api.TransactionType;
import org.killbill.billing.payment.plugin.api.PaymentPluginApiException;
import org.killbill.billing.payment.plugin.api.PaymentPluginStatus;
import org.killbill.billing.payment.plugin.api.PaymentTransactionInfoPlugin;
import org.killbill.billing.plugin.api.PluginCallContext;
import org.killbill.billing.plugin.core.PluginServlet;
import org.killbill.billing.plugin.ompay.client.OmPayHttpClient;
import org.killbill.billing.plugin.ompay.dao.OmPayDao;
import org.killbill.billing.tenant.api.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;

@Singleton
@Path("/process-nonce")
public class OmPayNonceHandlerServlet extends PluginServlet {

    private static final Logger logger = LoggerFactory.getLogger(OmPayNonceHandlerServlet.class);
    private final OmPayPaymentPluginApi paymentPluginApi;
    private final OmPayConfigurationHandler configurationHandler;
    private final OSGIKillbillAPI killbillAPI;
    private final OSGIKillbillClock clock;
    private final OmPayDao dao;
    private final OmPayHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Inject
    public OmPayNonceHandlerServlet(final OmPayPaymentPluginApi paymentPluginApi,
                                    final OmPayConfigurationHandler configurationHandler,
                                    final OSGIKillbillAPI killbillAPI,
                                    final OSGIKillbillClock clock,
                                    final OmPayDao dao) {
        this.paymentPluginApi = paymentPluginApi;
        this.configurationHandler = configurationHandler;
        this.killbillAPI = killbillAPI;
        this.clock = clock;
        this.dao = dao;
        this.httpClient = new OmPayHttpClient();
    }

    @POST
    public Result handleNonce(final Request req, @Local @Named("killbill_tenant") final Tenant tenant) {
        final PluginCallContext callContext = createPluginCallContext("process-nonce", tenant);

        try {
            // Extract payment method nonce from POST body
            final String paymentMethodNonce = req.param("nonce").value();
            if (Strings.isNullOrEmpty(paymentMethodNonce)) {
                logger.warn("Missing nonce in request");
                return Results.with("Required parameter 'nonce' missing", Status.BAD_REQUEST)
                        .header("Content-Type", "text/plain");
            }

            // Extract required parameters
            final String kbAccountIdString = req.param("kbAccountId").value();
            final String amountString = req.param("amount").value();
            final String currencyString = req.param("currency").value();
            final String paymentIntent = req.param("paymentIntent").value();

            // Optional parameters
            final String returnUrl = req.param("returnUrl").toOptional().orElse(null);
            final String cancelUrl = req.param("cancelUrl").toOptional().orElse(null);
            final String force3dsStr = req.param("force3ds").toOptional().orElse("false");

            // Validate required parameters
            if (Strings.isNullOrEmpty(kbAccountIdString) || Strings.isNullOrEmpty(amountString) ||
                    Strings.isNullOrEmpty(currencyString) || Strings.isNullOrEmpty(paymentIntent)) {
                logger.warn("Missing required parameters for nonce processing");
                return Results.with("Required parameters missing (kbAccountId, amount, currency, paymentIntent)", Status.BAD_REQUEST)
                        .header("Content-Type", "text/plain");
            }

            // Parse parameters
            final UUID kbAccountId = UUID.fromString(kbAccountIdString);
            final BigDecimal amount = new BigDecimal(amountString);
            final Currency currency = Currency.valueOf(currencyString.toUpperCase());
            final TransactionType transactionType = "sale".equalsIgnoreCase(paymentIntent) ?
                    TransactionType.PURCHASE : TransactionType.AUTHORIZE;

            // Generate payment and transaction IDs
            final UUID kbPaymentId = UUID.randomUUID();
            final UUID kbTransactionId = UUID.randomUUID();

            logger.info("Processing nonce for payment: kbAccountId={}, amount={}, currency={}, intent={}, kbPaymentId={}",
                    kbAccountId, amount, currency, transactionType, kbPaymentId);

            // Build properties to pass to authorize/purchase payment
            final List<PluginProperty> properties = new ArrayList<>();
            properties.add(new PluginProperty(OmPayPaymentPluginApi.PROPERTY_NONCE, paymentMethodNonce, false));

            if (!Strings.isNullOrEmpty(returnUrl)) {
                properties.add(new PluginProperty(OmPayPaymentPluginApi.PROPERTY_RETURN_URL, returnUrl, false));
            }
            if (!Strings.isNullOrEmpty(cancelUrl)) {
                properties.add(new PluginProperty(OmPayPaymentPluginApi.PROPERTY_CANCEL_URL, cancelUrl, false));
            }
            if (!Strings.isNullOrEmpty(force3dsStr)) {
                properties.add(new PluginProperty(OmPayPaymentPluginApi.PROPERTY_FORCE_3DS, force3dsStr, false));
            }

            // Call the appropriate payment method
            PaymentTransactionInfoPlugin transactionInfo;
            if (transactionType == TransactionType.AUTHORIZE) {
                transactionInfo = paymentPluginApi.authorizePayment(
                        kbAccountId, kbPaymentId, kbTransactionId, null, // kbPaymentMethodId is null for initial transaction
                        amount, currency, properties, callContext);
            } else {
                transactionInfo = paymentPluginApi.purchasePayment(
                        kbAccountId, kbPaymentId, kbTransactionId, null, // kbPaymentMethodId is null for initial transaction
                        amount, currency, properties, callContext);
            }

            // Build response based on transaction status
            final Map<String, Object> responseData = new HashMap<>();
            responseData.put("success", true);
            responseData.put("kb_payment_id", kbPaymentId.toString());
            responseData.put("kb_transaction_id", kbTransactionId.toString());
            responseData.put("transaction_type", transactionType.toString());
            responseData.put("status", transactionInfo.getStatus().toString());

            // Add transaction reference IDs if available
            if (transactionInfo.getFirstPaymentReferenceId() != null) {
                responseData.put("ompay_transaction_id", transactionInfo.getFirstPaymentReferenceId());
            }
            if (transactionInfo.getSecondPaymentReferenceId() != null) {
                responseData.put("ompay_reference_id", transactionInfo.getSecondPaymentReferenceId());
            }

            // Handle different payment statuses
            if (transactionInfo.getStatus() == PaymentPluginStatus.PENDING) {
                String redirectUrl = null;
                String authenticateUrl = null;

                if (transactionInfo.getProperties() != null) {
                    for (PluginProperty prop : transactionInfo.getProperties()) {
                        if ("redirect_url".equals(prop.getKey())) {
                            redirectUrl = String.valueOf(prop.getValue());
                        } else if ("authenticate_url".equals(prop.getKey())) {
                            authenticateUrl = String.valueOf(prop.getValue());
                        }
                    }
                }

                if (!Strings.isNullOrEmpty(redirectUrl)) {
                    responseData.put("requires_3ds", true);
                    responseData.put("redirect_url", redirectUrl);
                    logger.info("Payment requires 3DS authentication, redirecting to: {}", redirectUrl);

                    // For web flows, return JSON with redirect URL
                    // The frontend can then redirect the user
                    return Results.json(responseData).status(Status.OK);
                } else {
                    responseData.put("requires_3ds", false);
                    logger.info("Payment is pending but no redirect URL provided");
                }
            } else if (transactionInfo.getStatus() == PaymentPluginStatus.PROCESSED) {
                responseData.put("requires_3ds", false);
                logger.info("Payment processed successfully without 3DS");
            } else if (transactionInfo.getStatus() == PaymentPluginStatus.ERROR) {
                responseData.put("success", false);
                responseData.put("requires_3ds", false);
                responseData.put("error_message", transactionInfo.getGatewayError());
                responseData.put("error_code", transactionInfo.getGatewayErrorCode());
                logger.warn("Payment failed: {} ({})", transactionInfo.getGatewayError(), transactionInfo.getGatewayErrorCode());
            }

            return Results.json(responseData).status(Status.OK);

        } catch (PaymentPluginApiException e) {
            logger.error("Payment plugin error processing OMPay nonce:", e);
            final Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("error_message", e.getMessage());
            errorResponse.put("error_type", "payment_error");
            return Results.json(errorResponse).status(Status.SERVER_ERROR);

        } catch (IllegalArgumentException e) {
            logger.error("Invalid parameter processing OMPay nonce:", e);
            final Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("error_message", "Invalid parameters: " + e.getMessage());
            errorResponse.put("error_type", "validation_error");
            return Results.json(errorResponse).status(Status.BAD_REQUEST);

        } catch (Exception e) {
            logger.error("Unexpected error processing OMPay nonce:", e);
            final Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("error_message", "An unexpected server error occurred");
            errorResponse.put("error_type", "server_error");
            return Results.json(errorResponse).status(Status.SERVER_ERROR);
        }
    }

    private PluginCallContext createPluginCallContext(final String apiName, final Tenant tenant) {
        final UUID tenantId = (tenant != null) ? tenant.getId() : null;
        return new PluginCallContext(OmPayActivator.PLUGIN_NAME, clock.getClock().getUTCNow(), UUID.randomUUID(), tenantId);
    }
}