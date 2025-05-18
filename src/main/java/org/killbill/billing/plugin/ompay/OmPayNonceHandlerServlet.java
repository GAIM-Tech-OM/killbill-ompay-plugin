package org.killbill.billing.plugin.ompay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
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
    @Path("/process-nonce")
    public Result handleNonce(final HttpServletRequest request,
                              @Named("killbill_tenant") final Optional<Tenant> tenantOpt) {
        final PluginCallContext callContext = createPluginCallContext("process-nonce", tenantOpt.orElse(null), request);
        final OmPayConfigProperties config = configurationHandler.getConfigurable(callContext.getTenantId());

        final String paymentMethodNonce = request.getParameter("payment_method_nonce");
        final String kbAccountIdString = request.getParameter(OmPayPaymentPluginApi.PROPERTY_KB_ACCOUNT_ID);
        final String kbPaymentIdString = request.getParameter(OmPayPaymentPluginApi.PROPERTY_KB_PAYMENT_ID);
        final String kbTransactionIdString = request.getParameter(OmPayPaymentPluginApi.PROPERTY_KB_TRANSACTION_ID);
        final String amountString = request.getParameter(OmPayPaymentPluginApi.PROPERTY_AMOUNT);
        final String currencyString = request.getParameter(OmPayPaymentPluginApi.PROPERTY_CURRENCY);
        final String paymentIntent = request.getParameter(OmPayPaymentPluginApi.PROPERTY_PAYMENT_INTENT);

        if (Strings.isNullOrEmpty(paymentMethodNonce) || Strings.isNullOrEmpty(kbAccountIdString) ||
                Strings.isNullOrEmpty(kbPaymentIdString) || Strings.isNullOrEmpty(kbTransactionIdString) ||
                Strings.isNullOrEmpty(amountString) || Strings.isNullOrEmpty(currencyString) || Strings.isNullOrEmpty(paymentIntent)) {
            logger.warn("Missing required parameters for nonce processing.");
            return Results.with("Required parameters missing.", Status.BAD_REQUEST)
                    .header("Content-Type", "text/plain");
        }

        UUID kbAccountId = UUID.fromString(kbAccountIdString);
        UUID kbPaymentId = UUID.fromString(kbPaymentIdString);
        UUID kbTransactionId = UUID.fromString(kbTransactionIdString);
        BigDecimal amount = new BigDecimal(amountString);
        Currency currency = Currency.valueOf(currencyString.toUpperCase());
        TransactionType transactionType = "authorize".equalsIgnoreCase(paymentIntent) ? TransactionType.AUTHORIZE : TransactionType.PURCHASE;


        try {
            Account kbAccount = killbillAPI.getAccountUserApi().getAccountById(kbAccountId, callContext);

            Map<String, Object> paymentPayload = new HashMap<>();
            paymentPayload.put("intent", "sale".equalsIgnoreCase(paymentIntent) ? "auth" : "sale");
            paymentPayload.put("merchant_initiated", false);

            Map<String, Object> payer = new HashMap<>();
            payer.put("payment_type", "CC");
            Map<String, Object> fundingInstrument = new HashMap<>();
            Map<String, Object> creditCardNonceObj = new HashMap<>();
            creditCardNonceObj.put("nonce", paymentMethodNonce);
            fundingInstrument.put("credit_card_nonce", creditCardNonceObj);
            payer.put("funding_instrument", fundingInstrument);

            Map<String, Object> payerInfo = new HashMap<>();
            payerInfo.put("email", kbAccount.getEmail());
            Map<String, Object> billingAddress = new HashMap<>();
            billingAddress.put("line1", kbAccount.getAddress1());
            billingAddress.put("line2", kbAccount.getAddress2());
            billingAddress.put("city", kbAccount.getCity());
            billingAddress.put("country_code", kbAccount.getCountry());
            billingAddress.put("postal_code", kbAccount.getPostalCode());
            billingAddress.put("state", kbAccount.getStateOrProvince());
            payerInfo.put("billing_address", billingAddress);
            payer.put("payer_info", payerInfo);
            paymentPayload.put("payer", payer);

            Map<String, Object> payee = new HashMap<>();
            payee.put("merchant_id", config.getMerchantId());
            paymentPayload.put("payee", payee);

            Map<String, Object> transaction = new HashMap<>();
            transaction.put("type", 1);
            Map<String, Object> amountDetails = new HashMap<>();
            amountDetails.put("currency", currency.name());
            amountDetails.put("total", amount.toPlainString());
            transaction.put("amount", amountDetails);
            transaction.put("description", "Payment for KillBill: " + kbPaymentIdString);
            transaction.put("invoice_number", kbPaymentIdString);
            paymentPayload.put("transaction", transaction);

            String jsonPayload = objectMapper.writeValueAsString(paymentPayload);
            logger.debug("OMPay /payment request payload: {}", jsonPayload);

            OmPayHttpClient.OmPayHttpResponse omPayResponse = httpClient.doPost(
                    config.getApiBaseUrlWithMerchant() + "/payment",
                    jsonPayload,
                    config.getBasicAuthHeader(),
                    "application/json");

            Map<String, Object> omPayResponseMap = omPayResponse.getResponseMap();
            // ... (error handling for omPayResponseMap == null) ...
            if (omPayResponseMap == null) {
                logger.error("OMPay response could not be parsed or was empty. Status: {}, Body: {}", omPayResponse.getStatusCode(), omPayResponse.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Invalid response from OMPay gateway.");
            }
            logger.info("OMPay /payment response: {}", omPayResponse.getResponseBody());

            String ompayTransactionId = (String) omPayResponseMap.get("id");
            String ompayReferenceId = (String) omPayResponseMap.get("reference_id");
            String ompayState = (String) omPayResponseMap.get("state");
            Map<String, Object> omPayResult = (Map<String, Object>) omPayResponseMap.get("result");
            String ompayResultCode = omPayResult != null ? (String) omPayResult.get("code") : null;
            String ompayResultDescription = omPayResult != null ? (String) omPayResult.get("description") : null;
            String authenticateUrl = omPayResult != null ? (String) omPayResult.get("authenticate_url") : null;
            String redirectUrlFromResult = omPayResult != null ? (String) omPayResult.get("redirect_url") : null;


            if ("requires_action".equalsIgnoreCase(ompayState) || !Strings.isNullOrEmpty(authenticateUrl) || !Strings.isNullOrEmpty(redirectUrlFromResult)) {
                String finalRedirectUrl = !Strings.isNullOrEmpty(authenticateUrl) ? authenticateUrl : redirectUrlFromResult;
                logger.info("OMPay requires further action (e.g., 3DS). Redirecting to: {}", finalRedirectUrl);
                String extractedPayerIdFor3DS = null;
                Map<String, Object> payerResponseFor3DS = (Map<String, Object>) omPayResponseMap.get("payer");
                if (payerResponseFor3DS != null && payerResponseFor3DS.get("payer_info") instanceof Map) {
                    Map<String,Object> payerInfoRespFor3DS = (Map<String,Object>) payerResponseFor3DS.get("payer_info");
                    extractedPayerIdFor3DS = (String) payerInfoRespFor3DS.get("id");
                }
                try {
                    dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, transactionType, amount, currency,
                            ompayTransactionId, ompayReferenceId, extractedPayerIdFor3DS, null,
                            finalRedirectUrl, finalRedirectUrl,
                            omPayResponseMap, clock.getClock().getUTCNow(),  callContext.getTenantId());
                } catch (SQLException | JsonProcessingException dbEx) {
                    logger.error("DB Error storing pending 3DS transaction state for OMPay ID {}: {}", ompayTransactionId, dbEx.getMessage());
                }
                return Results.redirect(finalRedirectUrl);
            }


            List<PluginProperty> transactionProperties = new ArrayList<>();
            // ... (populate transactionProperties as before) ...
            transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_TRANSACTION_ID_PROP, ompayTransactionId, false));
            if (ompayReferenceId != null) transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_REFERENCE_ID_PROP, ompayReferenceId, false));
            transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_PAYMENT_STATE_PROP, ompayState, false));
            if (ompayResultCode != null) transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_RESULT_CODE_PROP, ompayResultCode, false));
            if (ompayResultDescription != null) transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_RESULT_DESC_PROP, ompayResultDescription, false));
            transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.RAW_OMPAY_RESPONSE_PROP, omPayResponse.getResponseBody(), false));


            String extractedOmpayPayerId = null; // Renamed to avoid conflict with method-scoped variable
            String ompayCardId = null;
            Map<String, Object> cardDetailsForPm = new HashMap<>();

            Map<String, Object> payerResponse = (Map<String, Object>) omPayResponseMap.get("payer");
            if (payerResponse != null) {
                if (payerResponse.get("payer_info") instanceof Map) {
                    Map<String,Object> payerInfoResp = (Map<String,Object>) payerResponse.get("payer_info");
                    extractedOmpayPayerId = (String) payerInfoResp.get("id"); // Extract Payer ID
                    if(extractedOmpayPayerId != null) {
                        transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_PAYER_ID_PROP, extractedOmpayPayerId, false));
                        // Add to cardDetailsForPm as well so it gets into PaymentMethodPlugin properties
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
                            cardDetailsForPm.put(OmPayPaymentPluginApi.OMPAY_CARD_ID_PROP, ompayCardId); // Though this is externalPaymentMethodId
                        }
                        if(ccResp.get("last4") != null) cardDetailsForPm.put("ompay_card_last4", ccResp.get("last4"));
                        if(ccResp.get("type") != null) cardDetailsForPm.put("ompay_card_type", ccResp.get("type"));
                        if(ccResp.get("expire_month") != null) cardDetailsForPm.put("ompay_card_expire_month", ccResp.get("expire_month"));
                        if(ccResp.get("expire_year") != null) cardDetailsForPm.put("ompay_card_expire_year", ccResp.get("expire_year"));
                        if(ccResp.get("name") != null) cardDetailsForPm.put("ompay_card_name", ccResp.get("name"));
                    }
                }
            }

            if (extractedOmpayPayerId != null) {
                boolean payerIdPropExists = false;
                for(PluginProperty p : transactionProperties) {
                    if(OmPayPaymentPluginApi.OMPAY_PAYER_ID_PROP.equals(p.getKey())) {
                        payerIdPropExists = true;
                        break;
                    }
                }
                if(!payerIdPropExists) {
                    transactionProperties.add(new PluginProperty(OmPayPaymentPluginApi.OMPAY_PAYER_ID_PROP, extractedOmpayPayerId, false));
                }
            }


            // Call the appropriate PaymentPluginApi method
            if (transactionType == TransactionType.AUTHORIZE) {
                paymentPluginApi.authorizePayment(kbAccountId, kbPaymentId, kbTransactionId, null, amount, currency, transactionProperties, callContext);
            } else {
                paymentPluginApi.purchasePayment(kbAccountId, kbPaymentId, kbTransactionId, null, amount, currency, transactionProperties, callContext);
            }

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
                // Pass transactionProperties to addPaymentMethod, it might contain OMPAY_PAYER_ID_PROP if not already in pmProps
                paymentPluginApi.addPaymentMethod(kbAccountId, kbPaymentMethodId, paymentMethodPlugin, true, transactionProperties, callContext);
            }

            // ... (redirect logic remains the same) ...
            String returnUrl = config.getKillbillBaseUrl() + "/plugins/ompay-plugin/payment-result" +
                    "?status=" + finalKbStatus +
                    "&kbPaymentId=" + kbPaymentIdString +
                    "&kbTransactionId=" + kbTransactionIdString +
                    "&ompayTransactionId=" + ompayTransactionId;
            logger.info("Payment processed for OMPay ID {}. Redirecting to: {}", ompayTransactionId, returnUrl);
            return Results.redirect(returnUrl);

        } catch (PaymentPluginApiException | AccountApiException | IOException e) {
            logger.error("Error processing OMPay nonce for account {}:", kbAccountIdString, e);
            return Results.with("Error processing payment: " + e.getMessage(), Status.SERVER_ERROR)
                    .header("Content-Type", "text/plain");
        } catch (Exception e) {
            logger.error("Unexpected error processing OMPay nonce for account {}:", kbAccountIdString, e);
            return Results.with("An unexpected server error occurred: " + e.getMessage(), Status.SERVER_ERROR)
                    .header("Content-Type", "text/plain");
        }
    }

    private PluginCallContext createPluginCallContext(final String apiName, final Tenant tenant, final HttpServletRequest request) {
        final UUID tenantId = (tenant != null) ? tenant.getId() : null;
        return new PluginCallContext(OmPayActivator.PLUGIN_NAME, clock.getClock().getUTCNow(),  UUID.randomUUID(), tenantId);
    }
}