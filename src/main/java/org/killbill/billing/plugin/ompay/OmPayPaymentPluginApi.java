package org.killbill.billing.plugin.ompay;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.joda.time.DateTime;
import org.killbill.billing.account.api.Account;
import org.killbill.billing.account.api.AccountApiException;
import org.killbill.billing.catalog.api.Currency;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillClock;
import org.killbill.billing.payment.api.PaymentMethodPlugin;
import org.killbill.billing.plugin.api.PluginCallContext;
import org.killbill.billing.plugin.api.PluginProperties;
import org.killbill.billing.plugin.api.core.PaymentApiWrapper;
import org.killbill.billing.payment.api.PluginProperty;
import org.killbill.billing.payment.api.TransactionStatus;
import org.killbill.billing.payment.api.TransactionType;
import org.killbill.billing.payment.plugin.api.*;
import org.killbill.billing.plugin.api.payment.PluginGatewayNotification;
import org.killbill.billing.plugin.api.payment.PluginHostedPaymentPageFormDescriptor;
import org.killbill.billing.plugin.api.payment.PluginPaymentTransactionInfoPlugin;

import org.killbill.billing.plugin.ompay.client.OmPayHttpClient;
import org.killbill.billing.plugin.ompay.dao.OmPayDao;
import org.killbill.billing.plugin.ompay.dao.gen.tables.records.OmpayPaymentMethodsRecord;
import org.killbill.billing.plugin.ompay.dao.gen.tables.records.OmpayResponsesRecord;
import org.killbill.billing.util.callcontext.CallContext;
import org.killbill.billing.util.callcontext.TenantContext;
import org.killbill.billing.util.entity.Pagination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable; // For Pagination
import java.io.IOException; // For Pagination close()
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Strings;


public class OmPayPaymentPluginApi implements PaymentPluginApi {

    private static final Logger logger = LoggerFactory.getLogger(OmPayPaymentPluginApi.class);
    private final OmPayConfigurationHandler configurationHandler;
    private final OSGIKillbillAPI killbillAPI;
    private final OSGIKillbillClock clock;
    private final OmPayDao dao;
    private final OmPayHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();


    public static final String PROPERTY_OMPAY_CLIENT_TOKEN = "ompayClientToken";
    public static final String PROPERTY_KILLBILL_BASE_URL = "killbillBaseUrl";
    public static final String OMPAY_TRANSACTION_ID_PROP = "ompay_transaction_id";
    public static final String OMPAY_REFERENCE_ID_PROP = "ompay_reference_id";
    public static final String OMPAY_PAYER_ID_PROP = "ompay_payer_id";
    public static final String OMPAY_CARD_ID_PROP = "ompay_card_id";
    public static final String OMPAY_PAYMENT_STATE_PROP = "ompay_payment_state";
    public static final String OMPAY_RESULT_CODE_PROP = "ompay_result_code";
    public static final String OMPAY_RESULT_DESC_PROP = "ompay_result_description";
    public static final String RAW_OMPAY_RESPONSE_PROP = "raw_ompay_response";

    // Additional properties for payment flow
    public static final String PROPERTY_NONCE = "nonce";
    public static final String PROPERTY_RETURN_URL = "returnUrl";
    public static final String PROPERTY_CANCEL_URL = "cancelUrl";
    public static final String PROPERTY_FORCE_3DS = "force3ds";


    public OmPayPaymentPluginApi(final OmPayConfigurationHandler configurationHandler,
                                 final OSGIKillbillAPI killbillAPI,
                                 final OSGIKillbillClock clock,
                                 final OmPayDao dao) {
        this.configurationHandler = configurationHandler;
        this.killbillAPI = killbillAPI;
        this.clock = clock;
        this.dao = dao;
        this.httpClient = new OmPayHttpClient();
    }

    @Override
    public HostedPaymentPageFormDescriptor buildFormDescriptor(final UUID kbAccountId,
                                                               final Iterable<PluginProperty> customFields,
                                                               final Iterable<PluginProperty> pluginProperties,
                                                               final CallContext context)
            throws PaymentPluginApiException {
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());

        // Get client token from OMPay
        String clientToken;
        try {
            final String clientTokenUrl = config.getApiBaseUrlWithMerchant() + "/client_token";
            String requestBody = "'grant_type=client_credentials'";

            logger.info("Requesting OMPay client token from: {}", clientTokenUrl);
            OmPayHttpClient.OmPayHttpResponse response = httpClient.doPost(clientTokenUrl, requestBody, config.getBasicAuthHeader(), "application/json");

            if (response.isSuccess() && response.getResponseMap() != null && response.getResponseMap().containsKey("accessToken")) {
                clientToken = (String) response.getResponseMap().get("accessToken");
                logger.info("Successfully retrieved OMPay client token");
            } else {
                String errorMsg = "Failed to get OMPay client token. Status: " + response.getStatusCode() + ", Body: " + response.getResponseBody();
                logger.error(errorMsg);
                throw new PaymentPluginApiException("OMPay API Error", errorMsg);
            }
        } catch (Exception e) {
            logger.error("Exception while getting OMPay client token", e);
            throw new PaymentPluginApiException("OMPay Network Error", "Exception retrieving client token: " + e.getMessage());
        }

        // Get Kill Bill base URL from properties or configuration
        String killbillBaseUrl = findPluginPropertyValue(PROPERTY_KILLBILL_BASE_URL, pluginProperties, config.getKillbillBaseUrl());
        if (Strings.isNullOrEmpty(killbillBaseUrl)) {
            logger.error("KillBill base URL is not configured for OMPay plugin");
            throw new PaymentPluginApiException("Configuration Error", "KillBill base URL is required");
        }

        // Form action URL for the payment form submission
        final String formActionUrl = killbillBaseUrl + "/plugins/" + OmPayActivator.PLUGIN_NAME + "/process-nonce";

        // Determine environment (sandbox or production)
        boolean isSandbox = config.isTestMode();

        // Extract fields from pluginProperties
        String amount = null;
        String currency = null;
        String paymentIntent = "auth"; // Default to auth if not specified
        String returnUrl = null;
        String cancelUrl = null;

        if (customFields != null) {
            for (PluginProperty field : customFields) {
                if ("amount".equals(field.getKey())) {
                    amount = (String) field.getValue();
                } else if ("currency".equals(field.getKey())) {
                    currency = (String) field.getValue();
                } else if ("paymentIntent".equals(field.getKey())) {
                    paymentIntent = (String) field.getValue();
                } else if ("returnUrl".equals(field.getKey())) {
                    returnUrl = (String) field.getValue();
                } else if ("cancelUrl".equals(field.getKey())) {
                    cancelUrl = (String) field.getValue();
                }
            }
        }

        // Build form properties with everything the frontend needs
        final List<PluginProperty> formProperties = new LinkedList<>();
        final List<PluginProperty> additional = new LinkedList<>();

        // Add hidden fields that should be passed to the form but not modified by user
        additional.add(new PluginProperty(PROPERTY_OMPAY_CLIENT_TOKEN, clientToken, false));
        additional.add(new PluginProperty("isSandbox", Boolean.toString(isSandbox), false));

        if (amount != null) {
            formProperties.add(new PluginProperty("amount", amount, false));
        }
        if (currency != null) {
            formProperties.add(new PluginProperty("currency", currency, false));
        }
        if (paymentIntent != null) {
            formProperties.add(new PluginProperty("paymentIntent", paymentIntent, false));
        }
        if (returnUrl != null) {
            formProperties.add(new PluginProperty("returnUrl", returnUrl, false));
        }
        if (cancelUrl != null) {
            formProperties.add(new PluginProperty("cancelUrl", cancelUrl, false));
        }

        return new PluginHostedPaymentPageFormDescriptor(kbAccountId, PluginHostedPaymentPageFormDescriptor.POST, formActionUrl, formProperties, additional);
    }



    @Override
    public PaymentTransactionInfoPlugin purchasePayment(final UUID kbAccountId,
                                                        final UUID kbPaymentId,
                                                        final UUID kbTransactionId,
                                                        final UUID kbPaymentMethodId,
                                                        final BigDecimal amount,
                                                        final Currency currency,
                                                        final Iterable<PluginProperty> properties,
                                                        final CallContext context) throws PaymentPluginApiException {

            return executeInitialTransaction(TransactionType.PURCHASE, kbAccountId, kbPaymentId, kbTransactionId,
                    kbPaymentMethodId, amount, currency, properties, context);
    }

    @Override
    public PaymentTransactionInfoPlugin authorizePayment(final UUID kbAccountId,
                                                         final UUID kbPaymentId,
                                                         final UUID kbTransactionId,
                                                         final UUID kbPaymentMethodId,
                                                         final BigDecimal amount,
                                                         final Currency currency,
                                                         final Iterable<PluginProperty> properties,
                                                         final CallContext context) throws PaymentPluginApiException {
            return executeInitialTransaction(TransactionType.AUTHORIZE, kbAccountId, kbPaymentId, kbTransactionId,
                    kbPaymentMethodId, amount, currency, properties, context);
    }

    /**
     * Execute initial transaction (authorize or purchase) with OMPay
     */
    private PaymentTransactionInfoPlugin executeInitialTransaction(final TransactionType transactionType,
                                                                   final UUID kbAccountId,
                                                                   final UUID kbPaymentId,
                                                                   final UUID kbTransactionId,
                                                                   final UUID kbPaymentMethodId,
                                                                   final BigDecimal amount,
                                                                   final Currency currency,
                                                                   final Iterable<PluginProperty> properties,
                                                                   final CallContext context) throws PaymentPluginApiException {

        logger.info("Executing {} for kbPaymentId: {}, kbTransactionId: {}",
                transactionType, kbPaymentId, kbTransactionId);

        // Check if this is a subsequent call (no nonce provided)
        final String nonce = findPluginPropertyValue(PROPERTY_NONCE, properties, null);

        // Determine if this is a subsequent transaction
        if (Strings.isNullOrEmpty(nonce) && kbPaymentMethodId != null) {
            // This is a subsequent transaction using stored payment method
            return executeSubsequentTransaction(transactionType, kbAccountId, kbPaymentId, kbTransactionId,
                    kbPaymentMethodId, amount, currency, properties, context);
        } else if (!Strings.isNullOrEmpty(nonce)) {
            // This is an initial transaction with nonce
            return executeOmPayPayment(transactionType, kbAccountId, kbPaymentId, kbTransactionId,
                    kbPaymentMethodId, amount, currency, properties, context);
        } else {
            // Just checking status - no actual payment execution
            return handleSubsequentTransactionCall(transactionType, kbAccountId, kbPaymentId,
                    kbTransactionId, amount, currency, properties, context);
        }
    }

    /**
     * Handle subsequent calls to authorize/purchase (without nonce) - just update status
     */
    private PaymentTransactionInfoPlugin handleSubsequentTransactionCall(final TransactionType transactionType,
                                                                         final UUID kbAccountId,
                                                                         final UUID kbPaymentId,
                                                                         final UUID kbTransactionId,
                                                                         final BigDecimal amount,
                                                                         final Currency currency,
                                                                         final Iterable<PluginProperty> properties,
                                                                         final CallContext context) throws PaymentPluginApiException {

        logger.info("Handling subsequent {} call for kbTransactionId: {}", transactionType, kbTransactionId);

        try {
            // Get existing transaction record
            List<PaymentTransactionInfoPlugin> existingTransactions = dao.getPaymentInfosForKbPaymentId(kbPaymentId, context.getTenantId());

            PaymentTransactionInfoPlugin existingTransaction = existingTransactions.stream()
                    .filter(txn -> txn.getKbTransactionPaymentId().equals(kbTransactionId))
                    .findFirst()
                    .orElse(null);

            if (existingTransaction == null) {
                throw new PaymentPluginApiException("Transaction Not Found",
                        "No existing transaction found for kbTransactionId: " + kbTransactionId);
            }

            // If transaction is still pending, check with OMPay for updates
            if (existingTransaction.getStatus() == PaymentPluginStatus.PENDING) {
                String ompayTransactionId = existingTransaction.getFirstPaymentReferenceId();
                if (!Strings.isNullOrEmpty(ompayTransactionId)) {
                    return refreshTransactionFromGateway(kbAccountId, ompayTransactionId, existingTransaction, context);
                }
            }

            // Return existing transaction info (already processed or failed)
            return existingTransaction;

        } catch (SQLException e) {
            logger.error("Error handling subsequent transaction call for kbTransactionId {}: {}",
                    kbTransactionId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Failed to retrieve transaction: " + e.getMessage());
        }
    }

    /**
     * Execute actual payment with OMPay API
     */
    private PaymentTransactionInfoPlugin executeOmPayPayment(final TransactionType transactionType,
                                                             final UUID kbAccountId,
                                                             final UUID kbPaymentId,
                                                             final UUID kbTransactionId,
                                                             final UUID kbPaymentMethodId,
                                                             final BigDecimal amount,
                                                             final Currency currency,
                                                             final Iterable<PluginProperty> properties,
                                                             final CallContext context) throws PaymentPluginApiException {

        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        final DateTime utcNow = clock.getClock().getUTCNow();

        // Extract properties
        final String nonce = findPluginPropertyValue(PROPERTY_NONCE, properties, null);
        final String returnUrl = findPluginPropertyValue(PROPERTY_RETURN_URL, properties, null);
        final String cancelUrl = findPluginPropertyValue(PROPERTY_CANCEL_URL, properties, null);
        final boolean force3ds = Boolean.parseBoolean(findPluginPropertyValue(PROPERTY_FORCE_3DS, properties, "false"));

        if (Strings.isNullOrEmpty(nonce)) {
            throw new PaymentPluginApiException("Missing Data", "Payment nonce is required for initial transaction.");
        }

        try {
            // Get account details
            final Account kbAccount = killbillAPI.getAccountUserApi().getAccountById(kbAccountId, context);

            // Build payment payload
            final Map<String, Object> paymentPayload = buildPaymentPayload(
                    transactionType, nonce, amount, currency, kbAccount, kbPaymentId,
                    returnUrl, cancelUrl, force3ds, config);

            // Call OMPay API
            final String jsonPayload = objectMapper.writeValueAsString(paymentPayload);
            logger.info("OMPay /payment request payload: {}", jsonPayload);

            final OmPayHttpClient.OmPayHttpResponse omPayResponse = httpClient.doPost(
                    config.getApiBaseUrlWithMerchant() + "/payment",
                    jsonPayload,
                    config.getBasicAuthHeader(),
                    "application/json");

            // Parse response
            final Map<String, Object> omPayResponseMap = omPayResponse.getResponseMap();
            if (omPayResponseMap == null) {
                logger.error("OMPay response could not be parsed. Status: {}, Body: {}",
                        omPayResponse.getStatusCode(), omPayResponse.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Invalid response from OMPay gateway.");
            }

            logger.info("OMPay /payment response for {}: {}", transactionType, omPayResponse.getResponseBody());

            // Extract response data
            final PaymentResponseData responseData = extractPaymentResponseData(omPayResponseMap);
            final PaymentPluginStatus pluginStatus = mapOmpayStatusToKillBill(responseData.state);

            // Store transaction in database
            dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, transactionType, amount, currency,
                    responseData.transactionId, responseData.referenceId, responseData.payerId,
                    responseData.cardId, responseData.state, responseData.redirectUrl, responseData.authenticateUrl,
                    omPayResponseMap, utcNow, context.getTenantId());

            // If transaction is successful (not pending), add payment method
            if (pluginStatus == PaymentPluginStatus.PROCESSED && !Strings.isNullOrEmpty(responseData.cardId)) {
                addPaymentMethodFromSuccessfulTransaction(kbAccountId, responseData, omPayResponseMap, context);
            }

            // Build and return transaction info
            return buildPaymentTransactionInfoPlugin(kbPaymentId, kbTransactionId, transactionType,
                    amount, currency, pluginStatus, responseData,
                    utcNow, properties);

        } catch (PaymentPluginApiException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error executing OMPay payment for {}: {}", transactionType, e.getMessage(), e);
            throw new PaymentPluginApiException("OMPay Payment Error", e.getMessage());
        }
    }


    /**
     * Extract payment response data from OMPay response (works for both payment and session responses)
     */
    private PaymentResponseData extractPaymentResponseData(final Map<String, Object> omPayResponseMap) {
        final PaymentResponseData data = new PaymentResponseData();

        data.transactionId = (String) omPayResponseMap.get("id");
        data.referenceId = (String) omPayResponseMap.get("reference_id");
        data.state = (String) omPayResponseMap.get("state");

        // Extract result information
        final Map<String, Object> result = (Map<String, Object>) omPayResponseMap.get("result");
        if (result != null) {
            data.resultCode = (String) result.get("code");
            data.resultDescription = (String) result.get("description");
            data.authenticateUrl = (String) result.get("authenticate_url");
            data.redirectUrl = (String) result.get("redirect_url");
        }

        // Extract payer and card information
        final Map<String, Object> payer = (Map<String, Object>) omPayResponseMap.get("payer");
        if (payer != null) {
            final Map<String, Object> payerInfo = (Map<String, Object>) payer.get("payer_info");
            if (payerInfo != null) {
                data.payerId = (String) payerInfo.get("id");
            }

            final Map<String, Object> fundingInstrument = (Map<String, Object>) payer.get("funding_instrument");
            if (fundingInstrument != null) {
                final Map<String, Object> creditCard = (Map<String, Object>) fundingInstrument.get("credit_card");
                if (creditCard != null) {
                    data.cardId = (String) creditCard.get("id");
                    data.cardDetails = new HashMap<>(creditCard); // Store full card details for payment method creation
                }
            }
        }

        return data;
    }

    /**
     * Execute subsequent transaction using stored payment method
     */
    private PaymentTransactionInfoPlugin executeSubsequentTransaction(final TransactionType transactionType,
                                                                      final UUID kbAccountId,
                                                                      final UUID kbPaymentId,
                                                                      final UUID kbTransactionId,
                                                                      final UUID kbPaymentMethodId,
                                                                      final BigDecimal amount,
                                                                      final Currency currency,
                                                                      final Iterable<PluginProperty> properties,
                                                                      final CallContext context) throws PaymentPluginApiException {

        logger.info("Executing subsequent {} transaction for kbPaymentId: {}, using stored payment method: {}",
                transactionType, kbPaymentId, kbPaymentMethodId);

        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        final DateTime utcNow = clock.getClock().getUTCNow();

        try {
            // Get stored payment method details
            final OmpayPaymentMethodsRecord pmRecord = dao.getPaymentMethodByKbPaymentMethodId(kbPaymentMethodId, context.getTenantId());
            if (pmRecord == null || pmRecord.getIsDeleted() == 1) {
                throw new PaymentPluginApiException("Payment Method Not Found",
                        "Payment method " + kbPaymentMethodId + " not found or deleted");
            }

            final String ompayCardId = pmRecord.getOmpayCreditCardId();

            final Account kbAccount = killbillAPI.getAccountUserApi().getAccountById(kbAccountId, context);

            // Build payment payload for subsequent transaction
            final Map<String, Object> paymentPayload = buildSubsequentPaymentPayload(
                    transactionType, ompayCardId,
                    amount, currency, kbAccount, kbPaymentId, config);

            // Call OMPay API
            final String jsonPayload = objectMapper.writeValueAsString(paymentPayload);
            logger.info("OMPay subsequent payment request payload: {}", jsonPayload);

            final OmPayHttpClient.OmPayHttpResponse omPayResponse = httpClient.doPost(
                    config.getApiBaseUrlWithMerchant() + "/payment",
                    jsonPayload,
                    config.getBasicAuthHeader(),
                    "application/json");

            // Parse and process response (same as initial transaction)
            final Map<String, Object> omPayResponseMap = omPayResponse.getResponseMap();
            if (omPayResponseMap == null) {
                logger.error("OMPay response could not be parsed. Status: {}, Body: {}",
                        omPayResponse.getStatusCode(), omPayResponse.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Invalid response from OMPay gateway.");
            }

            logger.info("OMPay subsequent payment response: {}", omPayResponse.getResponseBody());

            // Extract response data
            final PaymentResponseData responseData = extractPaymentResponseData(omPayResponseMap);
            final PaymentPluginStatus pluginStatus = mapOmpayStatusToKillBill(responseData.state);

            // Store transaction in database
            dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, transactionType, amount, currency,
                    responseData.transactionId, responseData.referenceId, responseData.payerId,
                    responseData.cardId, responseData.state, responseData.redirectUrl, responseData.authenticateUrl,
                    omPayResponseMap, utcNow, context.getTenantId());

            // Build and return transaction info
            return buildPaymentTransactionInfoPlugin(kbPaymentId, kbTransactionId, transactionType,
                    amount, currency, pluginStatus, responseData,
                    utcNow, properties);

        } catch (PaymentPluginApiException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error executing subsequent OMPay payment: {}", e.getMessage(), e);
            throw new PaymentPluginApiException("OMPay Payment Error", e.getMessage());
        }
    }

    /**
     * Build payment payload for initial transaction
     */
    private Map<String, Object> buildPaymentPayload(final TransactionType transactionType,
                                                    final String nonce,
                                                    final BigDecimal amount,
                                                    final Currency currency,
                                                    final Account kbAccount,
                                                    final UUID kbPaymentId,
                                                    final String returnUrl,
                                                    final String cancelUrl,
                                                    final boolean force3ds,
                                                    final OmPayConfigProperties config) {

        final Map<String, Object> payload = new HashMap<>();

        // Set intent based on transaction type
        payload.put("intent", transactionType == TransactionType.PURCHASE ? "sale" : "auth");

        // Payer section
        final Map<String, Object> payer = new HashMap<>();
        payer.put("payment_type", "CC");

        // Funding instrument with nonce
        final Map<String, Object> fundingInstrument = new HashMap<>();
        final Map<String, Object> creditCardNonce = new HashMap<>();
        creditCardNonce.put("nonce", nonce);
        fundingInstrument.put("credit_card_nonce", creditCardNonce);
        payer.put("funding_instrument", fundingInstrument);

        // Payer info
        final Map<String, Object> payerInfo = buildPayerInfo(kbAccount);
        payer.put("payer_info", payerInfo);
        payload.put("payer", payer);

        // Transaction section
        final Map<String, Object> transaction = new HashMap<>();
        transaction.put("type", "2");
        transaction.put("mode", force3ds ? 2 : 1); // 3DS mode

        // Amount
        final Map<String, Object> amountDetails = new HashMap<>();
        amountDetails.put("currency", currency.name());
        amountDetails.put("total", amount.toPlainString());
        transaction.put("amount", amountDetails);
        transaction.put("invoice_number", kbPaymentId.toString());

        // URLs for redirect flows
        if (!Strings.isNullOrEmpty(returnUrl)) {
            transaction.put("return_url", returnUrl);
        }
        if (!Strings.isNullOrEmpty(cancelUrl)) {
            transaction.put("cancel_url", cancelUrl);
        }

        payload.put("transaction", transaction);

        return payload;
    }

    /**
     * Build payment payload for subsequent transaction
     */
    private Map<String, Object> buildSubsequentPaymentPayload(final TransactionType transactionType,
                                                              final String ompayCardId,
                                                              final BigDecimal amount,
                                                              final Currency currency,
                                                              final Account kbAccount,
                                                              final UUID kbPaymentId,
                                                              final OmPayConfigProperties config) {

        final Map<String, Object> payload = new HashMap<>();

        // Set intent and merchant-initiated flags
        payload.put("intent", transactionType == TransactionType.PURCHASE ? "sale" : "auth");

        // Payer section
        final Map<String, Object> payer = new HashMap<>();
        payer.put("payment_type", "CC");

        // Funding instrument with stored card ID
        final Map<String, Object> fundingInstrument = new HashMap<>();
        final Map<String, Object> creditCardToken = new HashMap<>();
        creditCardToken.put("credit_card_id", ompayCardId);
        fundingInstrument.put("credit_card_token", creditCardToken);
        payer.put("funding_instrument", fundingInstrument);

        // Payer info (minimal for subsequent transactions)
        final Map<String, Object> payerInfo = new HashMap<>();
        payerInfo.put("email", kbAccount.getEmail());

        // Include billing address for subsequent transactions
        final Map<String, Object> billingAddress = buildBillingAddress(kbAccount);
        payerInfo.put("billing_address", billingAddress);

        payer.put("payer_info", payerInfo);
        payload.put("payer", payer);

        // Transaction section
        final Map<String, Object> transaction = new HashMap<>();
        transaction.put("type", "2");

        // Amount
        final Map<String, Object> amountDetails = new HashMap<>();
        amountDetails.put("currency", currency.name());
        amountDetails.put("total", amount.toPlainString());
        transaction.put("amount", amountDetails);
        transaction.put("invoice_number", kbPaymentId.toString());

        payload.put("transaction", transaction);

        try {
            logger.info("OMPay subsequent payment request payload before sending: {}", objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            logger.info("Could not serialize subsequent payment payload for logging", e);
        }

        return payload;
    }

    /**
     * Build payer info object
     */
    private Map<String, Object> buildPayerInfo(final Account kbAccount) {
        final Map<String, Object> payerInfo = new HashMap<>();
        payerInfo.put("email", kbAccount.getEmail());
        payerInfo.put("name", kbAccount.getName());

        final Map<String, Object> billingAddress = buildBillingAddress(kbAccount);
        payerInfo.put("billing_address", billingAddress);

        return payerInfo;
    }

    private Map<String, Object> buildBillingAddress(final Account kbAccount) {
        final Map<String, Object> billingAddress = new HashMap<>();
        billingAddress.put("country_code", "OM");
        return billingAddress;
    }

    /**
     * Override to ensure payment method is stored after successful initial transaction
     */
    private void addPaymentMethodFromSuccessfulTransaction(final UUID kbAccountId,
                                                           final PaymentResponseData responseData,
                                                           final Map<String, Object> omPayResponseMap,
                                                           final CallContext context) throws PaymentPluginApiException {

        if (Strings.isNullOrEmpty(responseData.cardId)) {
            logger.warn("Cannot add payment method: no card ID in response");
            return;
        }

        try {
            // Check if payment method already exists for this card
            List<OmpayPaymentMethodsRecord> existingPMs = dao.getPaymentMethods(kbAccountId, context.getTenantId());
            for (OmpayPaymentMethodsRecord pm : existingPMs) {
                if (responseData.cardId.equals(pm.getOmpayCreditCardId())) {
                    logger.info("Payment method already exists for card ID: {}", responseData.cardId);
                    return;
                }
            }

            final UUID kbPaymentMethodId = UUID.randomUUID();

            // Build additional data
            final Map<String, Object> pmAdditionalData = buildPaymentMethodAdditionalData(responseData, omPayResponseMap);

            // Add metadata about this being from an initial subscription payment
            pmAdditionalData.put("added_from_payment", true);
            pmAdditionalData.put("payment_state", responseData.state);
            pmAdditionalData.put("payment_result_code", responseData.resultCode);
            pmAdditionalData.put("is_subscription_payment_method", true);
            pmAdditionalData.put("initial_payment_id", responseData.transactionId);

            logger.info("Successfully added payment method {} from successful subscription transaction", kbPaymentMethodId);

            // Also add the payment method to Kill Bill
            final List<PluginProperty> pmProperties = pmAdditionalData.entrySet().stream()
                    .map(e -> new PluginProperty(e.getKey(), e.getValue(), false))
                    .collect(Collectors.toList());

            final PaymentMethodPlugin paymentMethodPlugin = new OmPayPaymentMethodPlugin(
                    kbPaymentMethodId,
                    responseData.cardId,
                    true,
                    pmProperties
            );

            killbillAPI.getPaymentApi().addPaymentMethod(
                    killbillAPI.getAccountUserApi().getAccountById(kbAccountId, context),
                    responseData.cardId,
                    OmPayActivator.PLUGIN_NAME,
                    true,
                    paymentMethodPlugin,
                    ImmutableList.of(),
                    context
            );

        } catch (Exception e) {
            logger.error("Error adding payment method from successful transaction: {}", e.getMessage(), e);
        }
    }


    /**
     * Refresh transaction status from OMPay gateway
     */
    private PaymentTransactionInfoPlugin refreshTransactionFromGateway(final UUID kbAccountId,
                                                                       final String ompayTransactionId,
                                                                       final PaymentTransactionInfoPlugin existingTransaction,
                                                                       final TenantContext context) throws PaymentPluginApiException {

        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());

        try {
            final String retrieveUrl = config.getApiBaseUrlWithMerchant() + "/payment/" + ompayTransactionId;
            final OmPayHttpClient.OmPayHttpResponse response = httpClient.doGet(retrieveUrl, config.getBasicAuthHeader());

            if (response.isSuccess() && response.getResponseMap() != null) {
                final Map<String, Object> updatedResponseMap = response.getResponseMap();
                final String newState = (String) updatedResponseMap.get("state");
                final PaymentPluginStatus newStatus = mapOmpayStatusToKillBill(newState);

                logger.info("Refreshed transaction {} status from {} to {}",
                        ompayTransactionId, existingTransaction.getStatus(), newStatus);

                // Update database with new status
                dao.updateResponseByOmPayTxnId(ompayTransactionId, newState, updatedResponseMap, context.getTenantId());

                // If status changed from pending to processed/error, notify Kill Bill
                if (existingTransaction.getStatus() == PaymentPluginStatus.PENDING && newStatus != PaymentPluginStatus.PENDING) {
                    notifyKillBillOfStatusChange(kbAccountId, existingTransaction, newStatus, context);
                }

                // Return updated transaction info
                return buildUpdatedTransactionInfo(existingTransaction, newStatus, updatedResponseMap);
            }

        } catch (Exception e) {
            logger.error("Error refreshing transaction {} from gateway: {}", ompayTransactionId, e.getMessage(), e);
        }

        // Return existing transaction if refresh failed
        return existingTransaction;
    }

    private void notifyKillBillOfStatusChange(final UUID accountId,
                                              final PaymentTransactionInfoPlugin transaction,
                                              final PaymentPluginStatus status,
                                              final TenantContext context) {
        try {
            final PaymentApiWrapper paymentApiWrapper = new PaymentApiWrapper(killbillAPI, false);
            final Account account = killbillAPI.getAccountUserApi().getAccountById(accountId, context);

            paymentApiWrapper.transitionPendingTransaction(account,
                    transaction.getKbPaymentId(),
                    transaction.getKbTransactionPaymentId(),
                    status,
                    (CallContext) context);

            logger.info("Notified Kill Bill of status change for transaction {}",
                    transaction.getKbTransactionPaymentId());

        } catch (Exception e) {
            logger.error("Error notifying Kill Bill of status change: {}", e.getMessage(), e);
        }
    }

    /**
     * Build updated transaction info with new status
     */
    private PaymentTransactionInfoPlugin buildUpdatedTransactionInfo(final PaymentTransactionInfoPlugin original,
                                                                     final PaymentPluginStatus newStatus,
                                                                     final Map<String, Object> updatedResponse) {

        final Map<String, Object> result = (Map<String, Object>) updatedResponse.get("result");
        final String errorCode = result != null ? (String) result.get("code") : null;
        final String errorMessage = result != null ? (String) result.get("description") : null;

        return new PluginPaymentTransactionInfoPlugin.Builder<>()
                .withKbPaymentId(original.getKbPaymentId())
                .withKbTransactionPaymentId(original.getKbTransactionPaymentId())
                .withTransactionType(original.getTransactionType())
                .withAmount(original.getAmount())
                .withCurrency(original.getCurrency())
                .withStatus(newStatus)
                .withGatewayError(errorMessage)
                .withGatewayErrorCode(errorCode)
                .withFirstPaymentReferenceId(original.getFirstPaymentReferenceId())
                .withSecondPaymentReferenceId(original.getSecondPaymentReferenceId())
                .withCreatedDate(original.getCreatedDate())
                .withEffectiveDate(original.getEffectiveDate())
                .withProperties(original.getProperties())
                .build();
    }

    /**
     * Build payment transaction info plugin
     */
    private PaymentTransactionInfoPlugin buildPaymentTransactionInfoPlugin(final UUID kbPaymentId,
                                                                           final UUID kbTransactionId,
                                                                           final TransactionType transactionType,
                                                                           final BigDecimal amount,
                                                                           final Currency currency,
                                                                           final PaymentPluginStatus status,
                                                                           final PaymentResponseData responseData,
                                                                           final DateTime utcNow,
                                                                           final Iterable<PluginProperty> originalProperties) {
        final List<PluginProperty> allProperties = new ArrayList<>();
        if (originalProperties != null) {
            originalProperties.forEach(allProperties::add);
        }
        if (!Strings.isNullOrEmpty(responseData.redirectUrl)) {
            allProperties.add(new PluginProperty("redirect_url", responseData.redirectUrl, false));
        }
        if (!Strings.isNullOrEmpty(responseData.authenticateUrl)) {
            allProperties.add(new PluginProperty("authenticate_url", responseData.authenticateUrl, false));
        }
        return new PluginPaymentTransactionInfoPlugin.Builder<>()
                .withKbPaymentId(kbPaymentId)
                .withKbTransactionPaymentId(kbTransactionId)
                .withTransactionType(transactionType)
                .withAmount(amount)
                .withCurrency(currency)
                .withStatus(status)
                .withGatewayError(responseData.resultDescription)
                .withGatewayErrorCode(responseData.resultCode)
                .withFirstPaymentReferenceId(responseData.transactionId)
                .withSecondPaymentReferenceId(responseData.referenceId)
                .withCreatedDate(utcNow)
                .withEffectiveDate(utcNow)
                .withProperties(allProperties)
                .build();
    }

    /**
     * Map OMPay status to Kill Bill status
     */
    public PaymentPluginStatus mapOmpayStatusToKillBill(String ompayState) {
        if (ompayState == null) return PaymentPluginStatus.UNDEFINED;

        switch (ompayState.toLowerCase()) {
            case "authorised":
            case "captured":
                return PaymentPluginStatus.PROCESSED;
            case "pending":
            case "requires_action":
                return PaymentPluginStatus.PENDING;
            case "declined":
            case "failed":
                return PaymentPluginStatus.ERROR;
            case "voided":
            case "cancelled":
                return PaymentPluginStatus.CANCELED;
            default:
                logger.warn("Unknown OMPay payment state received: {}", ompayState);
                return PaymentPluginStatus.UNDEFINED;
        }
    }

    /**
     * Find plugin property value
     */
    private String findPluginPropertyValue(String propertyKey, Iterable<PluginProperty> properties, String defaultValue) {
        if (properties != null) {
            for (PluginProperty prop : properties) {
                if (propertyKey.equals(prop.getKey()) && prop.getValue() != null) {
                    return prop.getValue().toString();
                }
            }
        }
        return defaultValue;
    }

    /**
     * Data class to hold payment response data
     */
    private static class PaymentResponseData {
        String transactionId;
        String referenceId;
        String state;
        String resultCode;
        String resultDescription;
        String payerId;
        String cardId;
        String redirectUrl;
        String authenticateUrl;
        Map<String, Object> cardDetails;
    }


    @Override
    public void addPaymentMethod(final UUID kbAccountId, final UUID kbPaymentMethodId, final PaymentMethodPlugin paymentMethodProps, final boolean setDefault, final Iterable<PluginProperty> properties, final CallContext context) throws PaymentPluginApiException {

        // Check if we have a sessionId (3DS flow completion)
        final String sessionId = findPluginPropertyValue("sessionId", properties, null);

        if (!Strings.isNullOrEmpty(sessionId)) {
            addPaymentMethodFromSession(kbAccountId, kbPaymentMethodId, sessionId, setDefault, properties, context);
        } else {
            addPaymentMethodDirect(kbAccountId, kbPaymentMethodId, paymentMethodProps, setDefault, properties, context);
        }
    }

    /**
     * Add payment method from 3DS session completion
     */
    private void addPaymentMethodFromSession(final UUID kbAccountId,
                                             final UUID kbPaymentMethodId,
                                             final String sessionId,
                                             final boolean setDefault,
                                             final Iterable<PluginProperty> properties,
                                             final CallContext context) throws PaymentPluginApiException {

        logger.info("Adding payment method from 3DS session: kbAccountId={}, sessionId={}", kbAccountId, sessionId);

        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());

        try {
            // Call OMPay API to get session details
            final String sessionUrl = config.getApiBaseUrlWithMerchant() + "/payment/session/" + sessionId;
            final OmPayHttpClient.OmPayHttpResponse response = httpClient.doGet(sessionUrl, config.getBasicAuthHeader());

            if (!response.isSuccess() || response.getResponseMap() == null) {
                logger.error("Failed to retrieve session details for sessionId: {}. Status: {}, Body: {}",
                        sessionId, response.getStatusCode(), response.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Failed to retrieve session details for sessionId: " + sessionId);
            }

            final Map<String, Object> sessionData = response.getResponseMap();
            logger.info("Retrieved session data for sessionId {}: {}", sessionId, response.getResponseBody());

            // Extract payment method details using existing parsing logic
            final PaymentResponseData responseData = extractPaymentResponseData(sessionData);

            if (Strings.isNullOrEmpty(responseData.cardId)) {
                throw new PaymentPluginApiException("Missing Data", "No card ID found in session data for sessionId: " + sessionId);
            }

            // Build additional data for payment method
            final Map<String, Object> additionalDataForPm = buildPaymentMethodAdditionalData(responseData, sessionData);
            dao.addPaymentMethod(kbAccountId, kbPaymentMethodId, responseData.cardId, responseData.payerId,
                    setDefault, additionalDataForPm, clock.getClock().getUTCNow(), context.getTenantId());
            logger.info("Successfully added payment method from 3DS session: kbId={}, ompayCardId={}, ompayPayerId={}",
                    kbPaymentMethodId, responseData.cardId, responseData.payerId);

            // --- BEGINNING OF NEW LOGIC TO UPDATE ORIGINAL TRANSACTION ---
            if (!Strings.isNullOrEmpty(responseData.transactionId)) { // responseData.transactionId is the OMPay ID of the original payment
                try {
                    OmpayResponsesRecord originalTransactionRecord = dao.getResponseByOmPayTransactionId(responseData.transactionId, context.getTenantId());

                    if (originalTransactionRecord != null) {
                        PaymentPluginStatus currentKbStatusInDb = PaymentPluginStatus.UNDEFINED;
                        try {
                            // Determine current KB status from the stored additional_data before update
                            if (!Strings.isNullOrEmpty(originalTransactionRecord.getAdditionalData())) {
                                Map<String, Object> currentAdditionalData = objectMapper.readValue(originalTransactionRecord.getAdditionalData(), new TypeReference<Map<String, Object>>() {});
                                String currentStateInDb = (String) currentAdditionalData.get("state");
                                currentKbStatusInDb = mapOmpayStatusToKillBill(currentStateInDb);
                            }
                        } catch (JsonProcessingException e) {
                            logger.warn("Could not parse current additional_data to determine status for OMPay transaction ID: {} before update from session.", responseData.transactionId, e);
                        }

                        PaymentPluginStatus newKbStatusFromSession = mapOmpayStatusToKillBill(responseData.state); // responseData.state is from the /payment/session/{id} call

                        logger.info("Original OMPay transaction {} (KB Txn ID: {}) found. Current KB status from DB: {}. New OMPay state from session: {} (New KB status: {})",
                                responseData.transactionId, originalTransactionRecord.getKbPaymentTransactionId(), currentKbStatusInDb, responseData.state, newKbStatusFromSession);

                        // Update the ompay_responses table's additional_data with the full sessionData,
                        // as it contains the final outcome of the payment.
                        dao.updateResponseByOmPayTxnId(responseData.transactionId, responseData.state, sessionData, context.getTenantId());
                        logger.info("Updated original ompay_responses record_id {} (OMPay ID {}) with final data from session {}.",
                                originalTransactionRecord.getRecordId(), responseData.transactionId, sessionId);

                        if (currentKbStatusInDb == PaymentPluginStatus.PENDING && newKbStatusFromSession != PaymentPluginStatus.PENDING) {
                            OmpayResponsesRecord updatedRecord = dao.getResponseByOmPayTransactionId(responseData.transactionId, context.getTenantId());
                            if (updatedRecord != null) {
                                PluginPaymentTransactionInfoPlugin transactionInfoForNotification = dao.toPaymentTransactionInfoPlugin(updatedRecord); // This now reflects the new state
                                notifyKillBillOfStatusChange(kbAccountId, transactionInfoForNotification, newKbStatusFromSession, context);
                            } else {
                                logger.error("Failed to re-fetch updated transaction record for OMPay ID {} for Kill Bill notification after session processing.", responseData.transactionId);
                            }
                        } else {
                            logger.info("No status change from PENDING or original status was not PENDING for OMPay transaction ID {}. Current DB status: {}, New session status: {}. No Kill Bill notification needed for this update path.",
                                    responseData.transactionId, currentKbStatusInDb, newKbStatusFromSession);
                        }

                    } else {
                        logger.warn("Could not find original transaction in ompay responses for OMPay transaction ID {} (obtained from session {}) to update its status.",
                                responseData.transactionId, sessionId);
                    }
                } catch (SQLException e) {
                    logger.error("Error updating original transaction (OMPay ID: {}) status from session {}: {}",
                            responseData.transactionId, sessionId, e.getMessage(), e);
                } catch (Exception e) {
                    logger.error("Unexpected error during original transaction update from session {} for OMPay transaction ID {}: {}",
                            sessionId, responseData.transactionId, e.getMessage(), e);
                }
            } else {
                logger.warn("No OMPay transaction ID (field 'id') found in session data for session ID {}. Cannot update original transaction status.", sessionId);
            }
            // --- END OF NEW LOGIC TO UPDATE ORIGINAL TRANSACTION ---


        } catch (PaymentPluginApiException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error adding payment method from session {}: {}", sessionId, e.getMessage(), e);
            throw new PaymentPluginApiException("OMPay Session Error", "Failed to add payment method from session: " + e.getMessage());
        }
    }

    /**
     * Add payment method with direct card details
     */
    private void addPaymentMethodDirect(final UUID kbAccountId,
                                        final UUID kbPaymentMethodId,
                                        final PaymentMethodPlugin paymentMethodProps,
                                        final boolean setDefault,
                                        final Iterable<PluginProperty> properties,
                                        final CallContext context) throws PaymentPluginApiException {

        final String ompayCreditCardId = paymentMethodProps.getExternalPaymentMethodId();
        if (Strings.isNullOrEmpty(ompayCreditCardId)) {
            throw new PaymentPluginApiException("Missing Data", "External (OMPay) credit card ID is required when not using sessionId.");
        }

        String ompayPayerId = null;
        final Map<String, Object> additionalDataMapForDb = new HashMap<>();

        // Extract properties from payment method
        if (paymentMethodProps.getProperties() != null) {
            for (PluginProperty prop : paymentMethodProps.getProperties()) {
                if (OMPAY_PAYER_ID_PROP.equals(prop.getKey())) {
                    ompayPayerId = String.valueOf(prop.getValue());
                } else {
                    additionalDataMapForDb.put(prop.getKey(), prop.getValue());
                }
            }
        }

        // Check additional properties for payer ID
        if (Strings.isNullOrEmpty(ompayPayerId) && properties != null) {
            ompayPayerId = findPluginPropertyValue(OMPAY_PAYER_ID_PROP, properties, null);
        }

        logger.info("Adding OMPay payment method directly: kbAccountId={}, kbPaymentMethodId={}, ompayCreditCardId={}, ompayPayerId={}, setDefault={}",
                kbAccountId, kbPaymentMethodId, ompayCreditCardId, ompayPayerId, setDefault);

        try {
            dao.addPaymentMethod(kbAccountId, kbPaymentMethodId, ompayCreditCardId, ompayPayerId, setDefault,
                    additionalDataMapForDb, clock.getClock().getUTCNow(), context.getTenantId());

            logger.info("Successfully added OMPay payment method directly (kbId: {}, ompayId: {}, ompayPayerId: {}) for account {}",
                    kbPaymentMethodId, ompayCreditCardId, ompayPayerId, kbAccountId);

        } catch (SQLException | JsonProcessingException e) {
            logger.error("Failed to add OMPay payment method for kbAccountId {}: {}", kbAccountId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Could not save OMPay payment method: " + e.getMessage());
        }
    }

    /**
     * Build additional data for payment method storage (reused from payment flow)
     */
    private Map<String, Object> buildPaymentMethodAdditionalData(final PaymentResponseData responseData,
                                                                 final Map<String, Object> sessionData) {
        final Map<String, Object> additionalData = new HashMap<>();

        // Add basic payment method info
        if (!Strings.isNullOrEmpty(responseData.payerId)) {
            additionalData.put(OMPAY_PAYER_ID_PROP, responseData.payerId);
        }

        // Add card details if available
        if (responseData.cardDetails != null) {
            additionalData.put("ompay_card_type", responseData.cardDetails.get("type"));
            additionalData.put("ompay_card_last4", responseData.cardDetails.get("last4"));
            additionalData.put("ompay_card_expire_month", responseData.cardDetails.get("expire_month"));
            additionalData.put("ompay_card_expire_year", responseData.cardDetails.get("expire_year"));

            if (responseData.cardDetails.get("bin") != null) {
                additionalData.put("ompay_card_bin", responseData.cardDetails.get("bin"));
            }

            // Add bin_data if available
            if (responseData.cardDetails.get("bin_data") instanceof Map) {
                additionalData.put("ompay_bin_data", responseData.cardDetails.get("bin_data"));
            }
        }

        // Add session completion metadata
        additionalData.put("added_from_session", true);
        additionalData.put("session_state", responseData.state);
        additionalData.put("session_result_code", responseData.resultCode);

        // Add transaction details if available
        final Map<String, Object> transaction = (Map<String, Object>) sessionData.get("transaction");
        if (transaction != null) {
            final Map<String, Object> amount = (Map<String, Object>) transaction.get("amount");
            if (amount != null) {
                additionalData.put("session_currency", amount.get("currency"));
                additionalData.put("session_amount", amount.get("total"));
            }
        }

        return additionalData;
    }

    @Override
    public PaymentTransactionInfoPlugin capturePayment(UUID kbAccountId, UUID kbPaymentId, UUID kbTransactionId, UUID kbPaymentMethodId, BigDecimal amount, Currency currency, Iterable<PluginProperty> properties, CallContext context) throws PaymentPluginApiException {
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        final DateTime utcNow = clock.getClock().getUTCNow();

        String originalAuthOmPayTxnId = findPluginPropertyValue(OMPAY_TRANSACTION_ID_PROP, properties, null);
        if (Strings.isNullOrEmpty(originalAuthOmPayTxnId)) {
            originalAuthOmPayTxnId = findPluginPropertyValue("ompay_original_transaction_id", properties, null);
        }

        if (Strings.isNullOrEmpty(originalAuthOmPayTxnId)) {
            try {
                List<OmpayResponsesRecord> authResponses = dao.getResponsesByKbPaymentIdAndType(kbPaymentId, TransactionType.AUTHORIZE, "authorised", context.getTenantId());
                if (!authResponses.isEmpty()) {
                    originalAuthOmPayTxnId = authResponses.get(0).getOmpayTransactionId();
                } else {
                    throw new PaymentPluginApiException("Missing Data", "Original OMPay authorization transaction ID (REFERENCE_ID) is required for capture and could not be found.");
                }
            } catch (SQLException e) {
                throw new PaymentPluginApiException("DB Error", "Failed to retrieve original authorization transaction for capture: " + e.getMessage());
            }
        }

        logger.info("Attempting to capture OMPay transaction ID: {} for kbTransactionId: {} with amount: {}", originalAuthOmPayTxnId, kbTransactionId, amount);

        String captureUrl = config.getApiBaseUrlWithMerchant() + "/payment/" + originalAuthOmPayTxnId + "/capture";
        Map<String, Object> payload = new HashMap<>();
        payload.put("amount", amount.toPlainString());
        String invoiceNumber = findPluginPropertyValue("invoice_number", properties, kbPaymentId.toString());
        payload.put("invoice_number", invoiceNumber);

        try {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            OmPayHttpClient.OmPayHttpResponse response = httpClient.doPost(captureUrl, jsonPayload, config.getBasicAuthHeader(), "application/json");
            Map<String, Object> responseMap = response.getResponseMap();

            if (responseMap == null) {
                logger.error("OMPay capture response could not be parsed or was empty. Status: {}, Body: {}", response.getStatusCode(), response.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Invalid response from OMPay gateway during capture.");
            }

            String newOmPayTxnId = (String) responseMap.get("id");
            String ompayState = (String) responseMap.get("state");
            Map<String, Object> omPayResult = (Map<String, Object>) responseMap.get("result");
            String resultCode = omPayResult != null ? (String) omPayResult.get("code") : null;
            String resultDescription = omPayResult != null ? (String) omPayResult.get("description") : "Capture Processed";

            PaymentPluginStatus status = mapOmpayStatusToKillBill(ompayState);
            if (!response.isSuccess() && status != PaymentPluginStatus.PROCESSED) {
                status = PaymentPluginStatus.ERROR;
            }

            dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, TransactionType.CAPTURE, amount, currency,
                    newOmPayTxnId, originalAuthOmPayTxnId,
                    null, null, null, null, null,
                    responseMap, utcNow, context.getTenantId());

            return new PluginPaymentTransactionInfoPlugin.Builder<>()
                    .withKbPaymentId(kbPaymentId)
                    .withKbTransactionPaymentId(kbTransactionId)
                    .withTransactionType(TransactionType.CAPTURE)
                    .withAmount(amount)
                    .withCurrency(currency)
                    .withStatus(status)
                    .withGatewayError(resultDescription)
                    .withGatewayErrorCode(resultCode)
                    .withFirstPaymentReferenceId(newOmPayTxnId)
                    .withSecondPaymentReferenceId(originalAuthOmPayTxnId)
                    .withCreatedDate(utcNow)
                    .withEffectiveDate(utcNow)
                    .withProperties(properties != null ? ImmutableList.copyOf(properties) : ImmutableList.of())
                    .build();

        } catch (PaymentPluginApiException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Exception during OMPay capture for originalTxnId {}: {}", originalAuthOmPayTxnId, e.getMessage(), e);
            throw new PaymentPluginApiException("OMPay Capture Error", e.getMessage());
        }
    }

    @Override
    public PaymentMethodPlugin getPaymentMethodDetail(final UUID kbAccountId, final UUID kbPaymentMethodId, final Iterable<PluginProperty> properties, final TenantContext context) throws PaymentPluginApiException {
        try {
            OmpayPaymentMethodsRecord record = dao.getPaymentMethodByKbPaymentMethodId(kbPaymentMethodId, context.getTenantId());
            if (record == null || record.getIsDeleted() == 1) {
                logger.info("Payment method not found or deleted for kbPaymentMethodId: {}", kbPaymentMethodId);
                return null;
            }

            Map<String, Object> additionalData = new HashMap<>();
            if (!Strings.isNullOrEmpty(record.getAdditionalData())) {
                try {
                    additionalData = objectMapper.readValue(record.getAdditionalData(), new TypeReference<Map<String, Object>>() {});
                } catch (JsonProcessingException e) {
                    logger.warn("Could not parse additionalData for payment method {}: {}", kbPaymentMethodId, e.getMessage());
                }
            }
            List<PluginProperty> pmProperties = additionalData.entrySet().stream()
                    .map(entry -> new PluginProperty(entry.getKey(), entry.getValue(), false))
                    .collect(Collectors.toList());

            return new OmPayPaymentMethodPlugin(
                    UUID.fromString(record.getKbPaymentMethodId()),
                    record.getOmpayCreditCardId(),
                    record.getIsDefault() == 1,
                    pmProperties
            );
        } catch (SQLException e) {
            logger.error("Error retrieving payment method detail for kbPaymentMethodId {}: {}", kbPaymentMethodId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Could not retrieve payment method detail: " + e.getMessage());
        }
    }

    @Override
    public Pagination<PaymentTransactionInfoPlugin> searchPayments(String searchKey, Long offset, Long limit, Iterable<PluginProperty> properties, TenantContext context) throws PaymentPluginApiException {
        if (Strings.isNullOrEmpty(searchKey)) {
            logger.warn("No search key provided for payment search");
            return getEmptyPaymentTransactionPagination(offset);
        }

        logger.info("Searching for payments with key: {}, offset: {}, limit: {}", searchKey, offset, limit);

        try {
            final List<PaymentTransactionInfoPlugin> results = dao.searchPayments(searchKey, offset, limit, context.getTenantId());

            return new Pagination<PaymentTransactionInfoPlugin>() {
                @Override public Long getCurrentOffset() { return offset; }
                @Override public Long getNextOffset() {
                    return results.size() < limit ? null : offset + limit;
                }
                @Override public Long getMaxNbRecords() { return (long) limit; }
                @Override public Long getTotalNbRecords() {
                    try {
                        return dao.getPaymentCount(searchKey, context.getTenantId());
                    } catch (SQLException e) {
                        logger.warn("Error getting total payment count", e);
                        return null;
                    }
                }
                @Override public Iterator<PaymentTransactionInfoPlugin> iterator() {
                    return results.iterator();
                }
                @Override public void close() throws IOException { /* No-op */ }
            };
        } catch (SQLException e) {
            logger.error("Error searching payments: {}", e.getMessage(), e);
            throw new PaymentPluginApiException("Search Error", "Failed to search payments: " + e.getMessage());
        }
    }

    @Override
    public Pagination<PaymentMethodPlugin> searchPaymentMethods(String searchKey, Long offset, Long limit, Iterable<PluginProperty> properties, TenantContext context) throws PaymentPluginApiException {
        if (Strings.isNullOrEmpty(searchKey)) {
            logger.warn("No search key provided for payment method search");
            return getEmptyPaymentMethodPagination(offset);
        }

        logger.info("Searching for payment methods with key: {}, offset: {}, limit: {}", searchKey, offset, limit);

        try {
            final List<PaymentMethodPlugin> results = dao.searchPaymentMethods(searchKey, offset, limit, context.getTenantId());

            return new Pagination<PaymentMethodPlugin>() {
                @Override public Long getCurrentOffset() { return offset; }
                @Override public Long getNextOffset() {
                    return results.size() < limit ? null : offset + limit;
                }
                @Override public Long getMaxNbRecords() { return (long) limit; }
                @Override public Long getTotalNbRecords() {
                    try {
                        return dao.getPaymentMethodCount(searchKey, context.getTenantId());
                    } catch (SQLException e) {
                        logger.warn("Error getting total payment method count", e);
                        return null;
                    }
                }
                @Override public Iterator<PaymentMethodPlugin> iterator() {
                    return results.iterator();
                }
                @Override public void close() throws IOException { /* No-op */ }
            };
        } catch (SQLException e) {
            logger.error("Error searching payment methods: {}", e.getMessage(), e);
            throw new PaymentPluginApiException("Search Error", "Failed to search payment methods: " + e.getMessage());
        }
    }

    private Pagination<PaymentMethodPlugin> getEmptyPaymentMethodPagination(final Long offset) {
        return new Pagination<PaymentMethodPlugin>() {
            @Override public Long getCurrentOffset() { return offset; }
            @Override public Long getNextOffset() { return null; }
            @Override public Long getMaxNbRecords() { return 0L; }
            @Override public Long getTotalNbRecords() { return 0L; }
            @Override public Iterator<PaymentMethodPlugin> iterator() {
                return ImmutableList.<PaymentMethodPlugin>of().iterator();
            }
            @Override public void close() throws IOException { /* No-op */ }
        };
    }

    private Pagination<PaymentTransactionInfoPlugin> getEmptyPaymentTransactionPagination(final Long offset) {
        return new Pagination<PaymentTransactionInfoPlugin>() {
            @Override public Long getCurrentOffset() { return offset; }
            @Override public Long getNextOffset() { return null; }
            @Override public Long getMaxNbRecords() { return 0L; }
            @Override public Long getTotalNbRecords() { return 0L; }
            @Override public Iterator<PaymentTransactionInfoPlugin> iterator() {
                return ImmutableList.<PaymentTransactionInfoPlugin>of().iterator();
            }
            @Override public void close() throws IOException { /* No-op */ }
        };
    }


    @Override
    public GatewayNotification processNotification(final String notificationBody, final Iterable<PluginProperty> properties, final CallContext context) throws PaymentPluginApiException {
        logger.info("Processing OMPay notification: {}", notificationBody);
        try {
            Map<String, Object> notificationMap = objectMapper.readValue(notificationBody, new TypeReference<Map<String, Object>>() {});

            // Extract core notification data
            String resourceType = (String) notificationMap.get("resource_type");
            String kind = (String) notificationMap.get("kind");
            String notificationId = (String) notificationMap.get("id");
            Map<String, Object> resource = (Map<String, Object>) notificationMap.get("resource");

            logger.info("Notification: id={}, type={}, kind={}", notificationId, resourceType, kind);

            if (resource == null) {
                logger.warn("Notification resource is null for notification ID: {}", notificationId);
                return new PluginGatewayNotification(notificationBody);
            }

            if ("payment".equals(resourceType)) {
                String ompayTransactionId = (String) resource.get("id");
                String ompayState = (String) resource.get("state");
                String ompayReferenceId = (String) resource.get("reference_id");

                if (Strings.isNullOrEmpty(ompayTransactionId)) {
                    logger.warn("Received OMPay payment notification with missing id: {}", notificationBody);
                    throw new PaymentPluginApiException("Invalid Notification", "Missing id in payment notification.");
                }

                // Try to find the transaction in our database
                OmpayResponsesRecord record = dao.getResponseByOmPayTransactionId(ompayTransactionId, context.getTenantId());

                // If not found by transaction ID, try reference ID (for captures, refunds, etc.)
                if (record == null && !Strings.isNullOrEmpty(ompayReferenceId)) {
                    logger.info("Transaction ID {} not found, trying reference ID {}", ompayTransactionId, ompayReferenceId);
                    record = dao.getResponseByOmPayTransactionId(ompayReferenceId, context.getTenantId());
                }

                if (record == null) {
                    // Still not found - this might be a notification for a transaction we don't know about
                    logger.warn("Received notification for unknown OMPay transaction ID: {} and reference ID: {}",
                            ompayTransactionId, ompayReferenceId);
                    return new PluginGatewayNotification(notificationBody);
                }

                UUID kbAccountId = UUID.fromString(record.getKbAccountId());
                UUID kbPaymentId = UUID.fromString(record.getKbPaymentId());
                UUID kbTransactionId = UUID.fromString(record.getKbPaymentTransactionId());
                PaymentPluginStatus newKbStatus = mapOmpayStatusToKillBill(ompayState);

                // Extract the result and additional data from the notification
                Map<String, Object> resultMap = (Map<String, Object>) resource.get("result");
                String resultCode = resultMap != null ? (String) resultMap.get("code") : null;
                String resultDescription = resultMap != null ? (String) resultMap.get("description") : null;

                // Get current transaction info to check if status has changed
                PluginPaymentTransactionInfoPlugin currentTxnInfo = dao.toPaymentTransactionInfoPlugin(record);

                if (currentTxnInfo.getStatus() != newKbStatus) {
                    logger.info("Updating transaction {} for OMPay ID {} from {} to {} based on notification kind: {}",
                            kbTransactionId, ompayTransactionId, currentTxnInfo.getStatus(), newKbStatus, kind);

                    // Update our database with the new status and details
                    Map<String, Object> updatedAdditionalData;
                    try {
                        updatedAdditionalData = objectMapper.readValue(record.getAdditionalData(),
                                new TypeReference<Map<String,Object>>() {});
                    } catch (JsonProcessingException e) {
                        updatedAdditionalData = new HashMap<>();
                    }

                    // Update with the latest information
                    updatedAdditionalData.put("state", ompayState);
                    updatedAdditionalData.put("notification_kind", kind);
                    updatedAdditionalData.put("notification_id", notificationId);
                    updatedAdditionalData.put("notification_processed_time", DateTime.now().toString());

                    if (resultMap != null) {
                        updatedAdditionalData.put("result", resultMap);
                    }

                    // If we have transaction details like amount, update them too
                    if (resource.get("transaction") instanceof Map) {
                        Map<String, Object> transactionMap = (Map<String, Object>) resource.get("transaction");
                        if (transactionMap.get("amount") instanceof Map) {
                            updatedAdditionalData.put("transaction", transactionMap);
                        }
                    }

                    // Save updated data to database
                    try {
                        dao.updateResponseAdditionalData(record.getRecordId(), ompayState,
                                objectMapper.writeValueAsString(updatedAdditionalData));
                    } catch (Exception e) {
                        logger.error("Error updating additional data for transaction {}: {}",
                                kbTransactionId, e.getMessage(), e);
                    }

                    // Call Kill Bill API to notify of state change if transaction was pending
                    if (currentTxnInfo.getStatus() == PaymentPluginStatus.PENDING) {
                        try {
                            Account killbillAccount = killbillAPI.getAccountUserApi().getAccountById(kbAccountId, context);
                            killbillAPI.getPaymentApi().notifyPendingTransactionOfStateChanged(
                                    killbillAccount,
                                    kbTransactionId,
                                    (newKbStatus == PaymentPluginStatus.PROCESSED), // isSuccess boolean
                                    context
                            );
                            logger.info("Notified Kill Bill of status change for transaction {}", kbTransactionId);
                        } catch (AccountApiException | org.killbill.billing.payment.api.PaymentApiException e) {
                            logger.error("Error notifying Kill Bill of transaction state change: {}", e.getMessage(), e);
                            throw new PaymentPluginApiException("Kill Bill API Error",
                                    "Failed to notify Kill Bill of transaction state change: " + e.getMessage());
                        }
                    } else {
                        logger.info("Transaction {} was not in PENDING state (was {}), not notifying Kill Bill",
                                kbTransactionId, currentTxnInfo.getStatus());
                    }
                } else {
                    logger.info("Notification for OMPay ID {} (kbTxnId {}). No status change: current={}, new={} (from '{}').",
                            ompayTransactionId, kbTransactionId, currentTxnInfo.getStatus(), newKbStatus, ompayState);
                }

                // Create plugin properties with notification details for the return value
                List<PluginProperty> notificationProperties = ImmutableList.of(
                        new PluginProperty("ompay_transaction_id", ompayTransactionId, false),
                        new PluginProperty("ompay_reference_id", ompayReferenceId, false),
                        new PluginProperty("processed_kb_transaction_id", kbTransactionId.toString(), false),
                        new PluginProperty("notification_kind", kind, false),
                        new PluginProperty("notification_id", notificationId, false),
                        new PluginProperty("new_status", newKbStatus.toString(), false),
                        new PluginProperty("result_code", resultCode, false),
                        new PluginProperty("result_description", resultDescription, false)
                );

                return new PluginGatewayNotification.Builder<>()
                        .withKbPaymentId(kbPaymentId)
                        .withEntity(notificationBody)
                        .withProperties(notificationProperties)
                        .build();
            } else {
                logger.warn("Received OMPay notification of unhandled resource_type '{}': {}",
                        resourceType, notificationBody);
            }
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse OMPay notification JSON: {}", notificationBody, e);
            throw new PaymentPluginApiException("Notification Parse Error",
                    "Invalid JSON in notification: " + e.getMessage());
        } catch (SQLException e) {
            logger.error("Database error while processing OMPay notification: {}", e.getMessage(), e);
            throw new PaymentPluginApiException("DB Error",
                    "Failed to process notification due to DB error: " + e.getMessage());
        }

        // Fallback if not handled or error before specific returns
        return new PluginGatewayNotification(notificationBody);
    }

    @Override
    public PaymentTransactionInfoPlugin voidPayment(final UUID kbAccountId, final UUID kbPaymentId, final UUID kbTransactionId, final UUID kbPaymentMethodId, final Iterable<PluginProperty> properties, final CallContext context) throws PaymentPluginApiException {
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        final DateTime utcNow = clock.getClock().getUTCNow();

        // The REFERENCE_ID is the OMPay transaction ID of the original authorization
        String originalOmPayTxnId = findPluginPropertyValue(OMPAY_TRANSACTION_ID_PROP, properties, null); // Try to get from properties
        if (Strings.isNullOrEmpty(originalOmPayTxnId)) {
            // If not in properties, try to find the most recent AUTHORIZE transaction for this kbPaymentId
            try {
                List<OmpayResponsesRecord> authResponses = dao.getResponsesByKbPaymentIdAndType(kbPaymentId, TransactionType.AUTHORIZE, "authorised", context.getTenantId());
                if (!authResponses.isEmpty()) {
                    originalOmPayTxnId = authResponses.get(0).getOmpayTransactionId(); // Assuming latest successful auth
                } else {
                    throw new PaymentPluginApiException("Missing Data", "Original OMPay authorization transaction ID (REFERENCE_ID) is required for void and could not be found.");
                }
            } catch (SQLException e) {
                throw new PaymentPluginApiException("DB Error", "Failed to retrieve original authorization transaction for void: " + e.getMessage());
            }
        }

        logger.info("Attempting to void OMPay transaction ID: {} for kbTransactionId: {}", originalOmPayTxnId, kbTransactionId);

        String voidUrl = config.getApiBaseUrlWithMerchant() + "/payment/" + originalOmPayTxnId + "/void";
        Map<String, Object> payload = new HashMap<>();
        // OMPay example payload for void includes invoice_number and custom fields.
        // You can extract these from properties or set defaults if needed.
        String invoiceNumber = findPluginPropertyValue("invoice_number", properties, kbPaymentId.toString()); // Example
        payload.put("invoice_number", invoiceNumber);
        // payload.put("custom", Map.of("field1", "Void from Kill Bill")); // Optional

        try {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            OmPayHttpClient.OmPayHttpResponse response = httpClient.doPost(voidUrl, jsonPayload, config.getBasicAuthHeader(), "application/json");
            Map<String, Object> responseMap = response.getResponseMap();

            if (responseMap == null) {
                logger.error("OMPay void response could not be parsed or was empty. Status: {}, Body: {}", response.getStatusCode(), response.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Invalid response from OMPay gateway during void.");
            }

            String newOmPayTxnId = (String) responseMap.get("id"); // OMPay returns a new transaction ID for the void operation
            String ompayState = (String) responseMap.get("state"); // Should be "voided"
            Map<String, Object> omPayResult = (Map<String, Object>) responseMap.get("result");
            String resultCode = omPayResult != null ? (String) omPayResult.get("code") : null;
            // OMPay void response example doesn't show a "description" in result, but good to check.
            String resultDescription = (omPayResult != null && omPayResult.get("description") != null) ? (String) omPayResult.get("description") : (omPayResult != null && omPayResult.get("message") != null ? (String) omPayResult.get("message") : "Void Processed");


            PaymentPluginStatus status = mapOmpayStatusToKillBill(ompayState);
            if (!response.isSuccess() && status != PaymentPluginStatus.CANCELED) { // CANCELED is a success for VOID in KB terms
                status = PaymentPluginStatus.ERROR; // If API call failed but didn't map to CANCELED
            }


            dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, TransactionType.VOID,
                    null, null, // Void typically doesn't have amount/currency in the new transaction record
                    newOmPayTxnId, originalOmPayTxnId, // newOmPayTxnId is firstRef, original is secondRef (or vice-versa based on your convention)
                    null, null, null, null, null,
                    responseMap, utcNow, context.getTenantId());

            return new PluginPaymentTransactionInfoPlugin.Builder<>()
                    .withKbPaymentId(kbPaymentId)
                    .withKbTransactionPaymentId(kbTransactionId)
                    .withTransactionType(TransactionType.VOID)
                    .withAmount(null)
                    .withCurrency(null)
                    .withStatus(status)
                    .withGatewayError(resultDescription)
                    .withGatewayErrorCode(resultCode)
                    .withFirstPaymentReferenceId(newOmPayTxnId)
                    .withSecondPaymentReferenceId(originalOmPayTxnId)
                    .withCreatedDate(utcNow)
                    .withEffectiveDate(utcNow)
                    .withProperties(properties != null ? ImmutableList.copyOf(properties) : ImmutableList.of())
                    .build();

        } catch (PaymentPluginApiException e) {
            throw e; // Re-throw if it's already the correct type
        } catch (Exception e) {
            logger.error("Exception during OMPay void for originalTxnId {}: {}", originalOmPayTxnId, e.getMessage(), e);
            throw new PaymentPluginApiException("OMPay Void Error", e.getMessage());
        }
    }

    @Override
    public PaymentTransactionInfoPlugin creditPayment(UUID kbAccountId, UUID kbPaymentId, UUID kbTransactionId, UUID kbPaymentMethodId, BigDecimal amount, Currency currency, Iterable<PluginProperty> properties, CallContext context) throws PaymentPluginApiException {
        return null;
    }

    @Override
    public PaymentTransactionInfoPlugin refundPayment(final UUID kbAccountId, final UUID kbPaymentId, final UUID kbTransactionId, final UUID kbPaymentMethodId, final BigDecimal amount, final Currency currency, final Iterable<PluginProperty> properties, final CallContext context) throws PaymentPluginApiException {
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        final DateTime utcNow = clock.getClock().getUTCNow();

        String originalOmPayTxnId = findPluginPropertyValue(OMPAY_TRANSACTION_ID_PROP, properties, null);
        if (Strings.isNullOrEmpty(originalOmPayTxnId)) {
            originalOmPayTxnId = findPluginPropertyValue("ompay_original_transaction_id", properties, null);
        }


        if (Strings.isNullOrEmpty(originalOmPayTxnId)) {
            // If not in properties, find the most recent CAPTURE or PURCHASE for this kbPaymentId
            try {
                List<OmpayResponsesRecord> successfulTransactions = new ArrayList<>();
                successfulTransactions.addAll(dao.getResponsesByKbPaymentIdAndType(kbPaymentId, TransactionType.CAPTURE, "captured", context.getTenantId()));
                if (successfulTransactions.isEmpty()) {
                    successfulTransactions.addAll(dao.getResponsesByKbPaymentIdAndType(kbPaymentId, TransactionType.PURCHASE, "captured", context.getTenantId())); // Assuming "captured" is the state for successful SALE
                    if (successfulTransactions.isEmpty()) {
                        successfulTransactions.addAll(dao.getResponsesByKbPaymentIdAndType(kbPaymentId, TransactionType.PURCHASE, "authorised", context.getTenantId())); // For some gateways sale might be "authorised"
                    }
                }

                if (!successfulTransactions.isEmpty()) {
                    originalOmPayTxnId = successfulTransactions.get(0).getOmpayTransactionId(); // Get the latest one
                } else {
                    throw new PaymentPluginApiException("Missing Data", "Original OMPay transaction ID (REFERENCE_ID) for refund could not be found.");
                }
            } catch (SQLException e) {
                throw new PaymentPluginApiException("DB Error", "Failed to retrieve original transaction for refund: " + e.getMessage());
            }
        }

        logger.info("Attempting to refund OMPay transaction ID: {} for kbTransactionId: {} with amount: {}", originalOmPayTxnId, kbTransactionId, amount);

        String refundUrl = config.getApiBaseUrlWithMerchant() + "/payment/" + originalOmPayTxnId + "/refund";
        Map<String, Object> payload = new HashMap<>();
        payload.put("amount", amount.toPlainString());
        String invoiceNumber = findPluginPropertyValue("invoice_number", properties, kbPaymentId.toString());
        payload.put("invoice_number", invoiceNumber);
        // payload.put("custom", Map.of("field1", "Refund from Kill Bill")); // Optional

        try {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            OmPayHttpClient.OmPayHttpResponse response = httpClient.doPost(refundUrl, jsonPayload, config.getBasicAuthHeader(), "application/json");
            Map<String, Object> responseMap = response.getResponseMap();

            if (responseMap == null) {
                logger.error("OMPay refund response could not be parsed or was empty. Status: {}, Body: {}", response.getStatusCode(), response.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Invalid response from OMPay gateway during refund.");
            }

            String newOmPayTxnId = (String) responseMap.get("id"); // OMPay returns a new transaction ID for the refund
            String ompayState = (String) responseMap.get("state"); // Should be "refunded"
            Map<String, Object> omPayResult = (Map<String, Object>) responseMap.get("result");
            String resultCode = omPayResult != null ? (String) omPayResult.get("code") : null;
            String resultDescription = omPayResult != null ? (String) omPayResult.get("description") : "Refund Processed";

            PaymentPluginStatus status = mapOmpayStatusToKillBill(ompayState);
            if (!response.isSuccess() && status != PaymentPluginStatus.PROCESSED) { // PROCESSED is success for REFUND
                status = PaymentPluginStatus.ERROR;
            }

            dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, TransactionType.REFUND, amount, currency,
                    newOmPayTxnId, originalOmPayTxnId,
                    null, null, null, null, null,
                    responseMap, utcNow, context.getTenantId());

            return new PluginPaymentTransactionInfoPlugin.Builder<>()
                    .withKbPaymentId(kbPaymentId)
                    .withKbTransactionPaymentId(kbTransactionId)
                    .withTransactionType(TransactionType.REFUND)
                    .withAmount(amount)
                    .withCurrency(currency)
                    .withStatus(status)
                    .withGatewayError(resultDescription)
                    .withGatewayErrorCode(resultCode)
                    .withFirstPaymentReferenceId(newOmPayTxnId)
                    .withSecondPaymentReferenceId(originalOmPayTxnId)
                    .withCreatedDate(utcNow)
                    .withEffectiveDate(utcNow)
                    .withProperties(properties != null ? ImmutableList.copyOf(properties) : ImmutableList.of())
                    .build();
        } catch (PaymentPluginApiException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Exception during OMPay refund for originalTxnId {}: {}", originalOmPayTxnId, e.getMessage(), e);
            throw new PaymentPluginApiException("OMPay Refund Error", e.getMessage());
        }
    }

    @Override
    public List<PaymentTransactionInfoPlugin> getPaymentInfo(final UUID kbAccountId, final UUID kbPaymentId, final Iterable<PluginProperty> properties, final TenantContext context) throws PaymentPluginApiException {
        logger.info("getPaymentInfo called for kbAccountId: {}, kbPaymentId: {}", kbAccountId, kbPaymentId);

        List<PaymentTransactionInfoPlugin> transactionsFromDb;
        try {
            transactionsFromDb = dao.getPaymentInfosForKbPaymentId(kbPaymentId, context.getTenantId());
        } catch (SQLException e) {
            logger.error("DAO Error fetching initial payment info for kbPaymentId {}: {}", kbPaymentId, e.getMessage(), e);
            throw new PaymentPluginApiException("DAO Error", "Could not retrieve payment info: " + e.getMessage());
        }

        if (transactionsFromDb.isEmpty()) {
            return transactionsFromDb;
        }

        boolean wasRefreshed = false;
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        Account account = null; // To be fetched if needed for notifications

        for (PaymentTransactionInfoPlugin transaction : transactionsFromDb) {
            if (transaction.getStatus() == PaymentPluginStatus.PENDING) {
                final String ompayTransactionIdToRefresh = transaction.getFirstPaymentReferenceId();

                if (Strings.isNullOrEmpty(ompayTransactionIdToRefresh)) {
                    logger.warn("Cannot refresh PENDING transaction kbTransactionId {} (OMPay ID is missing/null).", transaction.getKbTransactionPaymentId());
                    continue;
                }

                logger.info("Refreshing PENDING transaction: kbTransactionId={}, ompayTransactionId={}", transaction.getKbTransactionPaymentId(), ompayTransactionIdToRefresh);
                String retrieveUrl = config.getApiBaseUrlWithMerchant() + "/payment/" + ompayTransactionIdToRefresh;

                try {
                    OmPayHttpClient.OmPayHttpResponse response = httpClient.doGet(retrieveUrl, config.getBasicAuthHeader());
                    Map<String, Object> gatewayResponseMap = response.getResponseMap();

                    if (response.isSuccess() && gatewayResponseMap != null) {
                        String newStateFromGateway = (String) gatewayResponseMap.get("state");
                        PaymentPluginStatus newPluginStatus = mapOmpayStatusToKillBill(newStateFromGateway);

                        logger.info("Refreshed OMPay transaction ID {}: oldStatus={}, newGatewayState={}, newPluginStatus={}",
                                ompayTransactionIdToRefresh, transaction.getStatus(), newStateFromGateway, newPluginStatus);

                        // Update the local database record
                        dao.updateResponseByOmPayTxnId(ompayTransactionIdToRefresh, newStateFromGateway, gatewayResponseMap, context.getTenantId());
                        wasRefreshed = true;

                        // If status changed from PENDING to a terminal state, notify Kill Bill
                        if (transaction.getStatus() == PaymentPluginStatus.PENDING && newPluginStatus != PaymentPluginStatus.PENDING) {
                            OmpayResponsesRecord updatedRecord = dao.getResponseByOmPayTransactionId(ompayTransactionIdToRefresh, context.getTenantId());
                            if (updatedRecord != null) {
                                if (account == null) { // Fetch account only once if needed
                                    account = killbillAPI.getAccountUserApi().getAccountById(kbAccountId, context);
                                }
                                final CallContext callContextForNotification = new PluginCallContext(OmPayActivator.PLUGIN_NAME, clock.getClock().getUTCNow(), kbAccountId, context.getTenantId());

                                logger.info("Notifying Kill Bill of status change for kbTransactionId {} (OMPay ID {}) from PENDING to {}. isSuccess: {}",
                                        updatedRecord.getKbPaymentTransactionId(), ompayTransactionIdToRefresh, newPluginStatus, (newPluginStatus == PaymentPluginStatus.PROCESSED));

                                killbillAPI.getPaymentApi().notifyPendingTransactionOfStateChanged(
                                        account,
                                        UUID.fromString(updatedRecord.getKbPaymentTransactionId()),
                                        (newPluginStatus == PaymentPluginStatus.PROCESSED),
                                        callContextForNotification
                                );
                            } else {
                                logger.error("Failed to re-fetch updated record for OMPay ID {} after refresh, cannot notify Kill Bill.", ompayTransactionIdToRefresh);
                            }
                        }
                    } else {
                        logger.warn("Failed to refresh payment info for OMPay ID {} from gateway. Status: {}, Body: {}",
                                ompayTransactionIdToRefresh, response.getStatusCode(), response.getResponseBody());
                    }
                } catch (AccountApiException e) {
                    logger.error("Account API error while preparing for notification for OMPay ID {}: {}", ompayTransactionIdToRefresh, e.getMessage(), e);
                    // Potentially rethrow or handle if critical, for now, just log and continue
                } catch (org.killbill.billing.payment.api.PaymentApiException e) {
                    logger.error("Payment API error during Kill Bill notification for OMPay ID {}: {}", ompayTransactionIdToRefresh, e.getMessage(), e);
                    // Potentially rethrow or handle
                } catch (SQLException e) {
                    logger.error("DAO error during refresh or notification prep for OMPay ID {}: {}", ompayTransactionIdToRefresh, e.getMessage(), e);
                    // Don't let a single failed refresh stop others, but log it.
                } catch (Exception e) {
                    logger.error("Unexpected error during gateway refresh for OMPay ID {}: {}", ompayTransactionIdToRefresh, e.getMessage(), e);
                    // Don't let a single failed refresh stop others, but log it.
                }
            }
        }

        if (wasRefreshed) {
            try {
                // Re-fetch from DB to get all updates
                logger.info("Re-fetching payment info from DB after refresh for kbPaymentId: {}", kbPaymentId);
                return dao.getPaymentInfosForKbPaymentId(kbPaymentId, context.getTenantId());
            } catch (SQLException e) {
                logger.error("DAO Error fetching payment info after refresh for kbPaymentId {}: {}", kbPaymentId, e.getMessage(), e);
                throw new PaymentPluginApiException("DAO Error", "Could not retrieve payment info after refresh: " + e.getMessage());
            }
        } else {
            return transactionsFromDb;
        }
    }

    @Override
    public void deletePaymentMethod(UUID kbAccountId, UUID kbPaymentMethodId, Iterable<PluginProperty> properties, CallContext context) throws PaymentPluginApiException {
        logger.info("Attempting to delete payment method with kbPaymentMethodId: {}", kbPaymentMethodId);
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        OmpayPaymentMethodsRecord pmRecord = null;
        try {
            pmRecord = dao.getPaymentMethodByKbPaymentMethodId(kbPaymentMethodId, context.getTenantId());
        } catch (SQLException e) {
            logger.error("DB error retrieving payment method {} for deletion: {}", kbPaymentMethodId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Failed to retrieve payment method for deletion: " + e.getMessage());
        }

        if (pmRecord == null || pmRecord.getIsDeleted() == 1) {
            logger.info("Payment method {} already deleted or not found locally.", kbPaymentMethodId);
            return; // Nothing to do if not found or already marked deleted
        }

        String ompayCardId = pmRecord.getOmpayCreditCardId();
        String ompayPayerId = pmRecord.getOmpayPayerId(); // Assuming this column now exists and is populated

        if (Strings.isNullOrEmpty(ompayPayerId) && !Strings.isNullOrEmpty(pmRecord.getAdditionalData())) {
            // Fallback: try to get from additional_data if dedicated column wasn't populated/used
            try {
                Map<String, Object> additionalData = objectMapper.readValue(pmRecord.getAdditionalData(), new TypeReference<>() {});
                ompayPayerId = (String) additionalData.get(OMPAY_PAYER_ID_PROP);
            } catch (JsonProcessingException e) {
                logger.warn("Could not parse ompay_payer_id from payment method {} additional data for deletion.", kbPaymentMethodId);
            }
        }

        if (!Strings.isNullOrEmpty(ompayPayerId) && !Strings.isNullOrEmpty(ompayCardId)) {
            String deleteUrl = config.getApiBaseUrlWithMerchant() + "/payer/" + ompayPayerId + "/card/" + ompayCardId;
            logger.info("Calling OMPay to delete card: {}", deleteUrl);
            try {
                OmPayHttpClient.OmPayHttpResponse response = httpClient.doDelete(deleteUrl, config.getBasicAuthHeader());
                if (response.isSuccess() || response.getStatusCode() == 404) { // 404 might mean already deleted by OMPay
                    logger.info("OMPay card deletion successful (or card not found on gateway) for cardId {}, payerId {}.", ompayCardId, ompayPayerId);
                } else {
                    // Log OMPay's failure but still proceed to mark locally as deleted.
                    logger.warn("Failed to delete card from OMPay gateway for cardId {}: Status {}, Body: {}. Will mark local as deleted.",
                            ompayCardId, response.getStatusCode(), response.getResponseBody());
                }
            } catch (Exception e) {
                logger.error("Error calling OMPay delete card API for card {}: {}. Will mark local as deleted.", ompayCardId, e.getMessage(), e);
            }
        } else {
            logger.warn("Cannot delete card from OMPay gateway for kbPaymentMethodId {}: missing ompayPayerId or ompayCardId. Only marking locally.", kbPaymentMethodId);
        }

        try {
            dao.markPaymentMethodAsDeleted(kbPaymentMethodId, context.getTenantId());
            logger.info("Marked payment method {} as deleted in local DB.", kbPaymentMethodId);
        } catch (SQLException e) {
            logger.error("Error marking payment method {} as deleted in DB: {}", kbPaymentMethodId, e.getMessage());
            // If gateway deletion succeeded but local failed, this is problematic.
            // For now, rethrow. Consider more sophisticated retry/consistency mechanisms if needed.
            throw new PaymentPluginApiException("DB Error", "Failed to mark payment method as deleted locally: " + e.getMessage());
        }
    }

    @Override
    public void setDefaultPaymentMethod(UUID kbAccountId, UUID kbPaymentMethodId, Iterable<PluginProperty> properties, CallContext context) throws PaymentPluginApiException {
        logger.info("Setting payment method {} as default for account {}", kbPaymentMethodId, kbAccountId);
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        OmpayPaymentMethodsRecord pmRecord = null;

        try {
            pmRecord = dao.getPaymentMethodByKbPaymentMethodId(kbPaymentMethodId, context.getTenantId());
        } catch (SQLException e) {
            logger.error("DB error retrieving payment method {} to set as default: {}", kbPaymentMethodId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Failed to retrieve PM to set default: " + e.getMessage());
        }

        if (pmRecord == null || pmRecord.getIsDeleted() == 1) {
            logger.error("Cannot set non-existent or deleted payment method {} as default.", kbPaymentMethodId);
            throw new PaymentPluginApiException("Not Found", "Payment method to set as default not found or is deleted.");
        }

        String ompayCardId = pmRecord.getOmpayCreditCardId();
        String ompayPayerId = pmRecord.getOmpayPayerId();

        if (Strings.isNullOrEmpty(ompayPayerId) && !Strings.isNullOrEmpty(pmRecord.getAdditionalData())) {
            try {
                Map<String, Object> additionalData = objectMapper.readValue(pmRecord.getAdditionalData(), new TypeReference<>() {});
                ompayPayerId = (String) additionalData.get(OMPAY_PAYER_ID_PROP);
            } catch (JsonProcessingException e) { /* log */ }
        }

        if (!Strings.isNullOrEmpty(ompayPayerId) && !Strings.isNullOrEmpty(ompayCardId)) {
            String updateUrl = config.getApiBaseUrlWithMerchant() + "/payer/" + ompayPayerId;
            Map<String, String> payload = Map.of("default_card", ompayCardId);
            logger.info("Calling OMPay to set default card for payer {}: Card ID {}", ompayPayerId, ompayCardId);
            try {
                String jsonPayload = objectMapper.writeValueAsString(payload);
                OmPayHttpClient.OmPayHttpResponse response = httpClient.doPut(updateUrl, jsonPayload, config.getBasicAuthHeader(), "application/json");
                if (response.isSuccess()) {
                    logger.info("Successfully set default card {} for payer {} in OMPay.", ompayCardId, ompayPayerId);
                } else {
                    logger.warn("Failed to set default card in OMPay for payer {}. Status: {}, Body: {}. Will only update locally.",
                            ompayPayerId, response.getStatusCode(), response.getResponseBody());
                    // If OMPay fails, Kill Bill will still consider the local default.
                    // You might want to throw an exception here if strict gateway consistency is required.
                }
            } catch (Exception e) {
                logger.error("Error calling OMPay set default card API for payer {}: {}. Will only update locally.", ompayPayerId, e.getMessage(), e);
            }
        } else {
            logger.warn("Cannot set default card in OMPay for kbPaymentMethodId {}: missing ompayPayerId or ompayCardId. Updating locally only.", kbPaymentMethodId);
        }

        try {
            dao.clearDefault(kbAccountId, context.getTenantId());
            dao.setDefaultPaymentMethod(kbPaymentMethodId, context.getTenantId());
            logger.info("Successfully set payment method {} as default locally.", kbPaymentMethodId);
        } catch (SQLException e) {
            logger.error("Error setting payment method {} as default locally: {}", kbPaymentMethodId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Failed to set default payment method locally: " + e.getMessage());
        }
    }

    @Override
    public List<PaymentMethodInfoPlugin> getPaymentMethods(UUID kbAccountId, boolean refresh, Iterable<PluginProperty> properties, CallContext context) throws PaymentPluginApiException {
        logger.info("Getting payment methods for account: {}. Refresh: {}", kbAccountId, refresh);
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());

        if (refresh) {
            String ompayPayerId = null;
            try {
                // Attempt to get payer_id from any existing PM for this account
                ompayPayerId = dao.getOmpayPayerIdForAccount(kbAccountId, context.getTenantId());
            } catch (SQLException e) {
                logger.error("DB error trying to find OMPay Payer ID for account {} during refresh: {}", kbAccountId, e.getMessage());
                // Proceed without refresh if Payer ID cannot be determined
            }

            if (!Strings.isNullOrEmpty(ompayPayerId)) {
                String cardsUrl = config.getApiBaseUrlWithMerchant() + "/payer/" + ompayPayerId + "/card";
                logger.info("Refreshing payment methods from OMPay for payerId: {}", ompayPayerId);
                try {
                    OmPayHttpClient.OmPayHttpResponse response = httpClient.doGet(cardsUrl, config.getBasicAuthHeader());
                    if (response.isSuccess() && response.getResponseMap() != null) {
                        List<Map<String, Object>> ompayCards = (List<Map<String, Object>>) response.getResponseMap().get("credit_cards");
                        if (ompayCards != null) {
                            logger.info("Received {} cards from OMPay for payer {}", ompayCards.size(), ompayPayerId);
                            dao.synchronizePaymentMethods(kbAccountId, ompayPayerId, ompayCards, context.getTenantId(), clock.getClock().getUTCNow());
                            logger.info("Successfully synchronized payment methods for account {} with OMPay.", kbAccountId);
                        } else {
                            logger.warn("OMPay response for cards list was successful but 'credit_cards' field was missing or not a list. Payer ID: {}", ompayPayerId);
                            // It could be that the payer has no cards, which is a valid scenario.
                            // In this case, synchronizePaymentMethods should mark all local PMs (for this payer) as deleted.
                            dao.synchronizePaymentMethods(kbAccountId, ompayPayerId, Collections.emptyList(), context.getTenantId(), clock.getClock().getUTCNow());

                        }
                    } else {
                        logger.warn("Failed to fetch cards from OMPay for payerId {}. Status: {}, Body: {}",
                                ompayPayerId, response.getStatusCode(), response.getResponseBody());
                        // Don't fail the whole operation, just log and proceed to return local data.
                    }
                } catch (Exception e) {
                    logger.error("Error during OMPay payment methods refresh for payerId {}: {}", ompayPayerId, e.getMessage(), e);
                    // Fall through to return local data
                }
            } else {
                logger.warn("Cannot refresh payment methods from OMPay for account {}: OMPay Payer ID not found.", kbAccountId);
            }
        }

        try {
            List<OmpayPaymentMethodsRecord> records = dao.getPaymentMethods(kbAccountId, context.getTenantId());
            return records.stream()
                    .filter(record -> record.getIsDeleted() == 0) // Ensure not deleted
                    .map(record -> new OmPayPaymentMethodInfoPlugin(kbAccountId,
                            UUID.fromString(record.getKbPaymentMethodId()),
                            record.getIsDefault() == 1,
                            record.getOmpayCreditCardId()))
                    .collect(Collectors.toList());
        } catch (SQLException e) {
            logger.error("Error retrieving payment methods for account {}: {}", kbAccountId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Failed to retrieve payment methods: " + e.getMessage());
        }
    }

    @Override
    public void resetPaymentMethods(UUID kbAccountId, List<PaymentMethodInfoPlugin> paymentMethods, Iterable<PluginProperty> properties, CallContext context) throws PaymentPluginApiException {
        logger.warn("resetPaymentMethods is not supported by this OMPay plugin. This operation could lead to data inconsistencies if not carefully managed with OMPay's Payer-centric model.");
        throw new PaymentPluginApiException("Unsupported Operation", "resetPaymentMethods is not supported.");
    }
}