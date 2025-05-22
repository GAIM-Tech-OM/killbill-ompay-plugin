package org.killbill.billing.plugin.ompay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.jooby.Request;
import org.jooby.Result;
import org.jooby.Results;
import org.jooby.Status;
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
    public Result handleNonce(final Request req) {
        final PluginCallContext callContext = createPluginCallContext("process-nonce", null);
        final OmPayConfigProperties config = configurationHandler.getConfigurable(callContext.getTenantId());

        try {
            // Extract payment method nonce from POST body
            final String paymentMethodNonce = req.param("nonce").value();
            if (Strings.isNullOrEmpty(paymentMethodNonce)) {
                logger.warn("Missing nonce in request");
                return Results.with("Required parameter 'nonce' missing", Status.BAD_REQUEST)
                        .header("Content-Type", "text/plain");
            }

            // Extract query parameters
            final String kbAccountIdString = req.param("kbAccountId").value();
            final String amountString = req.param("amount").value();
            final String currencyString = req.param("currency").value();
            final String paymentIntent = req.param("paymentIntent").value();
            final String returnUrl = req.param("returnUrl").value();
            final String cancelUrl = req.param("cancelUrl").value();
            final boolean force3ds = Boolean.parseBoolean(req.param("force3ds").value());

            // Validate required parameters
            if (Strings.isNullOrEmpty(kbAccountIdString) || Strings.isNullOrEmpty(amountString) ||
                    Strings.isNullOrEmpty(currencyString)) {
                logger.warn("Missing required parameters for nonce processing");
                return Results.with("Required parameters missing (kbAccountId, amount, currency)", Status.BAD_REQUEST)
                        .header("Content-Type", "text/plain");
            }

            // Parse parameters
            UUID kbAccountId = UUID.fromString(kbAccountIdString);
            BigDecimal amount = new BigDecimal(amountString);
            Currency currency = Currency.valueOf(currencyString.toUpperCase());
            TransactionType transactionType = "sale".equalsIgnoreCase(paymentIntent) ?
                    TransactionType.PURCHASE : TransactionType.AUTHORIZE;

            // Generate payment and transaction IDs if not provided
            UUID kbPaymentId = UUID.randomUUID();
            UUID kbTransactionId = UUID.randomUUID();

            // Retrieve account details
            Account kbAccount = killbillAPI.getAccountUserApi().getAccountById(kbAccountId, callContext);

            // Build payment payload for OMPay
            Map<String, Object> paymentPayload = new HashMap<>();
            paymentPayload.put("intent", "sale".equalsIgnoreCase(paymentIntent) ? "sale" : "auth");
            paymentPayload.put("merchant_initiated", false);

            // Payer section
            Map<String, Object> payer = new HashMap<>();
            payer.put("payment_type", "CC");

            // Funding instrument - using the payment nonce
            Map<String, Object> fundingInstrument = new HashMap<>();
            Map<String, Object> creditCardNonceObj = new HashMap<>();
            creditCardNonceObj.put("nonce", paymentMethodNonce);
            fundingInstrument.put("credit_card_nonce", creditCardNonceObj);
            payer.put("funding_instrument", fundingInstrument);

            // Payer info and billing address
            Map<String, Object> payerInfo = new HashMap<>();
            payerInfo.put("email", kbAccount.getEmail());
            Map<String, Object> billingAddress = new HashMap<>();
            payerInfo.put("billing_address", billingAddress);
            payer.put("payer_info", payerInfo);
            paymentPayload.put("payer", payer);

            // Payee section
            Map<String, Object> payee = new HashMap<>();
            payee.put("merchant_id", config.getMerchantId());
            paymentPayload.put("payee", payee);

            // Transaction section
            Map<String, Object> transaction = new HashMap<>();
            transaction.put("type", "2"); // Type 2 is for subscriptions/recurring (per OMPay docs)

            // Amount details
            Map<String, Object> amountDetails = new HashMap<>();
            amountDetails.put("currency", currency.name());
            amountDetails.put("total", amount.toPlainString());
            transaction.put("amount", amountDetails);

            // Transaction description and invoice number
            transaction.put("description", "Payment for KillBill Account: " + kbAccountId);
            transaction.put("invoice_number", kbPaymentId.toString());

            // Add return_url and cancel_url if provided
            if (!Strings.isNullOrEmpty(returnUrl)) {
                transaction.put("return_url", returnUrl);
            }
            if (!Strings.isNullOrEmpty(cancelUrl)) {
                transaction.put("cancel_url", cancelUrl);
            }

            transaction.put("mode", force3ds ? 2 : 1);

            paymentPayload.put("transaction", transaction);

            // Convert payload to JSON
            String jsonPayload = objectMapper.writeValueAsString(paymentPayload);
            logger.debug("OMPay /payment request payload: {}", jsonPayload);

            // Send payment request to OMPay
            OmPayHttpClient.OmPayHttpResponse omPayResponse = httpClient.doPost(
                    config.getApiBaseUrlWithMerchant() + "/payment",
                    jsonPayload,
                    config.getBasicAuthHeader(),
                    "application/json");

            // Handle API response
            Map<String, Object> omPayResponseMap = omPayResponse.getResponseMap();
            if (omPayResponseMap == null) {
                logger.error("OMPay response could not be parsed or was empty. Status: {}, Body: {}",
                        omPayResponse.getStatusCode(), omPayResponse.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Invalid response from OMPay gateway.");
            }
            logger.info("OMPay /payment response: {}", omPayResponse.getResponseBody());

            // Extract important fields from the response
            String ompayTransactionId = (String) omPayResponseMap.get("id");
            String ompayReferenceId = (String) omPayResponseMap.get("reference_id");
            String ompayState = (String) omPayResponseMap.get("state");
            Map<String, Object> omPayResult = (Map<String, Object>) omPayResponseMap.get("result");
            String ompayResultCode = omPayResult != null ? (String) omPayResult.get("code") : null;
            String ompayResultDescription = omPayResult != null ? (String) omPayResult.get("description") : null;
            String authenticateUrl = omPayResult != null ? (String) omPayResult.get("authenticate_url") : null;
            String redirectUrlFromResult = omPayResult != null ? (String) omPayResult.get("redirect_url") : null;

            // For 3DS flows, we need to check if redirection is required
            boolean requires3DS = "requires_action".equalsIgnoreCase(ompayState) ||
                    !Strings.isNullOrEmpty(authenticateUrl) ||
                    !Strings.isNullOrEmpty(redirectUrlFromResult);

            if (requires3DS) {
                String finalRedirectUrl = !Strings.isNullOrEmpty(authenticateUrl) ?
                        authenticateUrl : redirectUrlFromResult;
                logger.info("OMPay requires further action (3DS). Redirecting to: {}", finalRedirectUrl);

                // Extract payer ID for recording
                String extractedPayerIdFor3DS = null;
                Map<String, Object> payerResponseFor3DS = (Map<String, Object>) omPayResponseMap.get("payer");
                if (payerResponseFor3DS != null && payerResponseFor3DS.get("payer_info") instanceof Map) {
                    Map<String,Object> payerInfoRespFor3DS = (Map<String,Object>) payerResponseFor3DS.get("payer_info");
                    extractedPayerIdFor3DS = (String) payerInfoRespFor3DS.get("id");
                }

                // Store the pending 3DS transaction
                try {
                    dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, transactionType, amount, currency,
                            ompayTransactionId, ompayReferenceId, extractedPayerIdFor3DS, null,
                            finalRedirectUrl, finalRedirectUrl,
                            omPayResponseMap, clock.getClock().getUTCNow(), callContext.getTenantId());
                } catch (SQLException | JsonProcessingException dbEx) {
                    logger.error("DB Error storing pending 3DS transaction state for OMPay ID {}: {}",
                            ompayTransactionId, dbEx.getMessage());
                }

                // Return the redirect URL to the client
                Map<String, Object> responseData = new HashMap<>();
                responseData.put("requires_3ds", true);
                responseData.put("redirect_url", finalRedirectUrl);
                responseData.put("ompay_transaction_id", ompayTransactionId);
                responseData.put("kb_payment_id", kbPaymentId.toString());
                responseData.put("kb_transaction_id", kbTransactionId.toString());

                return Results.json(responseData).status(Status.OK);
            }

            // For non-3DS flows, record the payment
            List<PluginProperty> transactionProperties = new ArrayList<>();
            transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_TRANSACTION_ID_PROP, ompayTransactionId, false));
            if (ompayReferenceId != null) {
                transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_REFERENCE_ID_PROP, ompayReferenceId, false));
            }
            transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_PAYMENT_STATE_PROP, ompayState, false));
            if (ompayResultCode != null) {
                transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_RESULT_CODE_PROP, ompayResultCode, false));
            }
            if (ompayResultDescription != null) {
                transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_RESULT_DESC_PROP, ompayResultDescription, false));
            }
            transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.RAW_OMPAY_RESPONSE_PROP, omPayResponse.getResponseBody(), false));

            // Extract payer and card details
            String extractedOmpayPayerId = null;
            String ompayCardId = null;
            Map<String, Object> cardDetailsForPm = new HashMap<>();

            Map<String, Object> payerResponse = (Map<String, Object>) omPayResponseMap.get("payer");
            if (payerResponse != null) {
                if (payerResponse.get("payer_info") instanceof Map) {
                    Map<String,Object> payerInfoResp = (Map<String,Object>) payerResponse.get("payer_info");
                    extractedOmpayPayerId = (String) payerInfoResp.get("id");
                    if(extractedOmpayPayerId != null) {
                        transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_PAYER_ID_PROP, extractedOmpayPayerId, false));
                        cardDetailsForPm.put(OmPayPaymentPluginApi.OMPAY_PAYER_ID_PROP, extractedOmpayPayerId);
                    }
                }
                if (payerResponse.get("funding_instrument") instanceof Map) {
                    Map<String, Object> fundingInstrumentResp = (Map<String, Object>) payerResponse.get("funding_instrument");
                    if (fundingInstrumentResp.get("credit_card") instanceof Map) {
                        Map<String, Object> ccResp = (Map<String, Object>) fundingInstrumentResp.get("credit_card");
                        ompayCardId = (String) ccResp.get("id");
                        if(ompayCardId != null) {
                            transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_CARD_ID_PROP, ompayCardId, false));
                            cardDetailsForPm.put(OmPayPaymentPluginApi.OMPAY_CARD_ID_PROP, ompayCardId);
                        }
                        if(ccResp.get("last4") != null) cardDetailsForPm.put("ompay_card_last4", ccResp.get("last4"));
                        if(ccResp.get("type") != null) cardDetailsForPm.put("ompay_card_type", ccResp.get("type"));
                        if(ccResp.get("expire_month") != null) cardDetailsForPm.put("ompay_card_expire_month", ccResp.get("expire_month"));
                        if(ccResp.get("expire_year") != null) cardDetailsForPm.put("ompay_card_expire_year", ccResp.get("expire_year"));
                        if(ccResp.get("name") != null) cardDetailsForPm.put("ompay_card_name", ccResp.get("name"));
                    }
                }
            }

            // Record the payment
            if (transactionType == TransactionType.AUTHORIZE) {
                paymentPluginApi.authorizePayment(kbAccountId, kbPaymentId, kbTransactionId, null,
                        amount, currency, transactionProperties, callContext);
            } else {
                paymentPluginApi.purchasePayment(kbAccountId, kbPaymentId, kbTransactionId, null,
                        amount, currency, transactionProperties, callContext);
            }

            // If payment is successful and we have card details, save the payment method
            PaymentPluginStatus finalKbStatus = paymentPluginApi.mapOmpayStatusToKillBill(ompayState);
            if (finalKbStatus == PaymentPluginStatus.PROCESSED && !Strings.isNullOrEmpty(ompayCardId)) {
                UUID kbPaymentMethodId = UUID.randomUUID();
                List<PluginProperty> pmProps = new ArrayList<>();
                for(Map.Entry<String, Object> entry : cardDetailsForPm.entrySet()){
                    if(entry.getValue() != null) {
                        pmProps.add(new PluginProperty(entry.getKey(), entry.getValue(), false));
                    }
                }

                PaymentMethodPlugin paymentMethodPlugin = new OmPayPaymentMethodPlugin(
                        kbPaymentMethodId,
                        ompayCardId,
                        true,
                        pmProps
                );

                // Save the payment method
                paymentPluginApi.addPaymentMethod(kbAccountId, kbPaymentMethodId, paymentMethodPlugin,
                        true, transactionProperties, callContext);
            }

            // Return success response with payment details
            Map<String, Object> responseData = new HashMap<>();
            responseData.put("success", true);
            responseData.put("requires_3ds", false);
            responseData.put("ompay_transaction_id", ompayTransactionId);
            responseData.put("ompay_state", ompayState);
            responseData.put("kb_payment_id", kbPaymentId.toString());
            responseData.put("kb_transaction_id", kbTransactionId.toString());

            return Results.json(responseData).status(Status.OK);

        } catch (PaymentPluginApiException | AccountApiException | IOException e) {
            logger.error("Error processing OMPay nonce:", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("error_message", e.getMessage());
            return Results.json(errorResponse).status(Status.SERVER_ERROR);
        } catch (Exception e) {
            logger.error("Unexpected error processing OMPay nonce:", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("error_message", "An unexpected server error occurred");
            return Results.json(errorResponse).status(Status.SERVER_ERROR);
        }
    }

    private PluginCallContext createPluginCallContext(final String apiName, final Tenant tenant) {
        final UUID tenantId = (tenant != null) ? tenant.getId() : null;
        return new PluginCallContext(OmPayActivator.PLUGIN_NAME, clock.getClock().getUTCNow(), UUID.randomUUID(), tenantId);
    }
}