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
    public static final String PROPERTY_OMPAY_FORM_ACTION_URL = "ompayFormActionUrl";
    public static final String PROPERTY_OMPAY_FORM_HTML_CONTENT = "ompayFormHtmlContent";
    public static final String PROPERTY_KILLBILL_BASE_URL = "killbillBaseUrl";
    public static final String PROPERTY_KB_ACCOUNT_ID = "kbAccountId";
    public static final String PROPERTY_KB_PAYMENT_ID = "kbPaymentId";
    public static final String PROPERTY_KB_TRANSACTION_ID = "kbTransactionId";
    public static final String PROPERTY_AMOUNT = "amount";
    public static final String PROPERTY_CURRENCY = "currency";
    public static final String PROPERTY_PAYMENT_INTENT = "paymentIntent"; // "auth" or "sale"
    public static final String OMPAY_TRANSACTION_ID_PROP = "ompay_transaction_id";
    public static final String OMPAY_REFERENCE_ID_PROP = "ompay_reference_id";
    public static final String OMPAY_PAYER_ID_PROP = "ompay_payer_id";
    public static final String OMPAY_CARD_ID_PROP = "ompay_card_id";
    public static final String OMPAY_PAYMENT_STATE_PROP = "ompay_payment_state";
    public static final String OMPAY_RESULT_CODE_PROP = "ompay_result_code";
    public static final String OMPAY_RESULT_DESC_PROP = "ompay_result_description";
    public static final String RAW_OMPAY_RESPONSE_PROP = "raw_ompay_response";


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

        String clientToken;
        try {
            final String clientTokenUrl = config.getApiBaseUrlWithMerchant() + "/client_token";
            Map<String, Object> bodyMap = new HashMap<>();
            bodyMap.put("grant_type", "client_credentials");
            String requestBody = objectMapper.writeValueAsString(bodyMap);

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

        // Determine environment (sandbox or production)
        boolean isSandbox = config.isTestMode();

        // Form action URL for the payment form submission
        final String formActionUrl = killbillBaseUrl + "/plugins/" + OmPayActivator.PLUGIN_NAME + "/process-nonce";

        // Build form properties with everything the frontend needs to render the form
        final List<PluginProperty> formProperties = new LinkedList<>();
        formProperties.add(new PluginProperty(PROPERTY_OMPAY_CLIENT_TOKEN, clientToken, false));
        formProperties.add(new PluginProperty(PROPERTY_OMPAY_FORM_ACTION_URL, formActionUrl, false));
        formProperties.add(new PluginProperty(PROPERTY_KB_ACCOUNT_ID, kbAccountId.toString(), false));
        formProperties.add(new PluginProperty("is_sandbox", Boolean.toString(isSandbox), false));

        return new PluginHostedPaymentPageFormDescriptor(kbAccountId, formActionUrl, formProperties);
    }


    @Override
    public PaymentTransactionInfoPlugin purchasePayment(final UUID kbAccountId, final UUID kbPaymentId, final UUID kbTransactionId, final UUID kbPaymentMethodId, final BigDecimal amount, final Currency currency, final Iterable<PluginProperty> properties, final CallContext context) throws PaymentPluginApiException {
        String ompayTransactionId = findPluginPropertyValue(OMPAY_TRANSACTION_ID_PROP, properties, null);
        String ompayReferenceId = findPluginPropertyValue(OMPAY_REFERENCE_ID_PROP, properties, null);
        String ompayPayerId = findPluginPropertyValue(OMPAY_PAYER_ID_PROP, properties, null);
        String ompayCardId = findPluginPropertyValue(OMPAY_CARD_ID_PROP, properties, null);
        String ompayPaymentState = findPluginPropertyValue(OMPAY_PAYMENT_STATE_PROP, properties, "UNKNOWN");
        String ompayResultCode = findPluginPropertyValue(OMPAY_RESULT_CODE_PROP, properties, null);
        String ompayResultDescription = findPluginPropertyValue(OMPAY_RESULT_DESC_PROP, properties, null);
        String rawOmPayResponse = findPluginPropertyValue(RAW_OMPAY_RESPONSE_PROP, properties, "{}");

        if (Strings.isNullOrEmpty(ompayTransactionId)) {
            throw new PaymentPluginApiException("Missing Data", "ompay_transaction_id is required for purchasePayment.");
        }

        PaymentPluginStatus status = mapOmpayStatusToKillBill(ompayPaymentState);
        Map<String, Object> additionalDataFromResponse; // Renamed to avoid conflict
        try {
            additionalDataFromResponse = objectMapper.readValue(rawOmPayResponse, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            logger.warn("Could not parse raw OMPay response for purchase: {}", rawOmPayResponse, e);
            additionalDataFromResponse = Map.of("error", "Failed to parse OMPay response", "rawResponse", rawOmPayResponse);
        }

        final DateTime utcNow = clock.getClock().getUTCNow();
        try {
            dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, TransactionType.PURCHASE, amount, currency,
                    ompayTransactionId, ompayReferenceId, ompayPayerId, ompayCardId,
                    null, null,
                    additionalDataFromResponse, utcNow, context.getTenantId());
        } catch (SQLException | JsonProcessingException e) {
            throw new PaymentPluginApiException("DB Error", "Failed to record OMPay purchase response: " + e.getMessage());
        }

        // Using the Builder from the provided PluginPaymentTransactionInfoPlugin source
        return new PluginPaymentTransactionInfoPlugin.Builder<>()
                .withKbPaymentId(kbPaymentId)
                .withKbTransactionPaymentId(kbTransactionId)
                .withTransactionType(TransactionType.PURCHASE)
                .withAmount(amount)
                .withCurrency(currency)
                .withStatus(status)
                .withGatewayError(ompayResultDescription)
                .withGatewayErrorCode(ompayResultCode)
                .withFirstPaymentReferenceId(ompayTransactionId) // OMPay's main transaction ID
                .withSecondPaymentReferenceId(ompayReferenceId) // OMPay's reference_id
                .withCreatedDate(utcNow)
                .withEffectiveDate(utcNow)
                .withProperties(properties != null ? ImmutableList.copyOf(properties) : ImmutableList.of())
                .build();
    }

    @Override
    public PaymentTransactionInfoPlugin authorizePayment(UUID kbAccountId, UUID kbPaymentId, UUID kbTransactionId, UUID kbPaymentMethodId, BigDecimal amount, Currency currency, Iterable<PluginProperty> properties, CallContext context) throws PaymentPluginApiException {
        String ompayTransactionId = findPluginPropertyValue(OMPAY_TRANSACTION_ID_PROP, properties, null);
        String ompayReferenceId = findPluginPropertyValue(OMPAY_REFERENCE_ID_PROP, properties, null);
        String ompayPayerId = findPluginPropertyValue(OMPAY_PAYER_ID_PROP, properties, null);
        String ompayCardId = findPluginPropertyValue(OMPAY_CARD_ID_PROP, properties, null);
        String ompayPaymentState = findPluginPropertyValue(OMPAY_PAYMENT_STATE_PROP, properties, "UNKNOWN");
        String ompayResultCode = findPluginPropertyValue(OMPAY_RESULT_CODE_PROP, properties, null);
        String ompayResultDescription = findPluginPropertyValue(OMPAY_RESULT_DESC_PROP, properties, null);
        String rawOmPayResponse = findPluginPropertyValue(RAW_OMPAY_RESPONSE_PROP, properties, "{}");

        if (Strings.isNullOrEmpty(ompayTransactionId)) {
            throw new PaymentPluginApiException("Missing Data", "ompay_transaction_id is required for authorizePayment.");
        }
        PaymentPluginStatus status = mapOmpayStatusToKillBill(ompayPaymentState);
        Map<String, Object> additionalDataFromResponse;
        try {
            additionalDataFromResponse = objectMapper.readValue(rawOmPayResponse, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            logger.warn("Could not parse raw OMPay response for auth: {}", rawOmPayResponse, e);
            additionalDataFromResponse = Map.of("error", "Failed to parse OMPay response", "rawResponse", rawOmPayResponse);
        }

        final DateTime utcNow = clock.getClock().getUTCNow();
        try {
            dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, TransactionType.AUTHORIZE, amount, currency,
                    ompayTransactionId, ompayReferenceId, ompayPayerId, ompayCardId,
                    null, null, additionalDataFromResponse, utcNow, context.getTenantId());
        } catch (SQLException | JsonProcessingException e) {
            throw new PaymentPluginApiException("DB Error", "Failed to record OMPay auth response: " + e.getMessage());
        }

        return new PluginPaymentTransactionInfoPlugin.Builder<>()
                .withKbPaymentId(kbPaymentId)
                .withKbTransactionPaymentId(kbTransactionId)
                .withTransactionType(TransactionType.AUTHORIZE)
                .withAmount(amount)
                .withCurrency(currency)
                .withStatus(status)
                .withGatewayError(ompayResultDescription)
                .withGatewayErrorCode(ompayResultCode)
                .withFirstPaymentReferenceId(ompayTransactionId)
                .withSecondPaymentReferenceId(ompayReferenceId)
                .withCreatedDate(utcNow)
                .withEffectiveDate(utcNow)
                .withProperties(properties != null ? ImmutableList.copyOf(properties) : ImmutableList.of())
                .build();
    }


    @Override
    public void addPaymentMethod(final UUID kbAccountId, final UUID kbPaymentMethodId, final PaymentMethodPlugin paymentMethodProps, final boolean setDefault, final Iterable<PluginProperty> properties, final CallContext context) throws PaymentPluginApiException {
        String ompayCreditCardId = paymentMethodProps.getExternalPaymentMethodId();
        if (Strings.isNullOrEmpty(ompayCreditCardId)) {
            throw new PaymentPluginApiException("Missing Data", "External (OMPay) credit card ID is required.");
        }

        String ompayPayerId = null;
        Map<String, Object> additionalDataMapForDb = new HashMap<>(); // For other details to store in JSON

        if (paymentMethodProps.getProperties() != null) {
            for (PluginProperty prop : paymentMethodProps.getProperties()) {
                if (OMPAY_PAYER_ID_PROP.equals(prop.getKey())) {
                    ompayPayerId = String.valueOf(prop.getValue());
                } else {
                    additionalDataMapForDb.put(prop.getKey(), prop.getValue());
                }
            }
        }

        if (Strings.isNullOrEmpty(ompayPayerId) && properties != null) {
            ompayPayerId = findPluginPropertyValue(OMPAY_PAYER_ID_PROP, properties, null);
        }

        logger.info("Adding OMPay payment method: kbAccountId={}, kbPaymentMethodId={}, ompayCreditCardId={}, ompayPayerId={}, setDefault={}",
                kbAccountId, kbPaymentMethodId, ompayCreditCardId, ompayPayerId, setDefault);

        try {
            dao.addPaymentMethod(kbAccountId, kbPaymentMethodId, ompayCreditCardId, ompayPayerId, setDefault,
                    additionalDataMapForDb, // Pass the map without ompay_payer_id if it's a dedicated column
                    clock.getClock().getUTCNow(), context.getTenantId());
            logger.info("Successfully added OMPay payment method (kbId: {}, ompayId: {}, ompayPayerId: {}) for account {}",
                    kbPaymentMethodId, ompayCreditCardId, ompayPayerId, kbAccountId);
        } catch (SQLException | JsonProcessingException e) { // JsonProcessingException from DAO is now just SQLException
            logger.error("Failed to add OMPay payment method for kbAccountId {}: {}", kbAccountId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Could not save OMPay payment method: " + e.getMessage());
        }
    }


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
            case "voided": // OMPay might use "cancelled" or "voided"
            case "cancelled":
                return PaymentPluginStatus.CANCELED;
            default:
                logger.warn("Unknown OMPay payment state received: {}", ompayState);
                return PaymentPluginStatus.UNDEFINED;
        }
    }


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


    // Modify in OmPayPaymentPluginApi.java

    @Override
    public PaymentTransactionInfoPlugin capturePayment(UUID kbAccountId, UUID kbPaymentId, UUID kbTransactionId, UUID kbPaymentMethodId, BigDecimal amount, Currency currency, Iterable<PluginProperty> properties, CallContext context) throws PaymentPluginApiException {
        final OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
        final DateTime utcNow = clock.getClock().getUTCNow();

        String originalAuthOmPayTxnId = findPluginPropertyValue(OMPAY_TRANSACTION_ID_PROP, properties, null); // If capture is for a specific OMPay txn
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
        // payload.put("custom", Map.of("field1", "Capture from Kill Bill")); // Optional

        try {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            OmPayHttpClient.OmPayHttpResponse response = httpClient.doPost(captureUrl, jsonPayload, config.getBasicAuthHeader(), "application/json");
            Map<String, Object> responseMap = response.getResponseMap();

            if (responseMap == null) {
                logger.error("OMPay capture response could not be parsed or was empty. Status: {}, Body: {}", response.getStatusCode(), response.getResponseBody());
                throw new PaymentPluginApiException("OMPay API Error", "Invalid response from OMPay gateway during capture.");
            }

            String newOmPayTxnId = (String) responseMap.get("id"); // OMPay returns a new transaction ID for the capture
            String ompayState = (String) responseMap.get("state"); // Should be "captured"
            Map<String, Object> omPayResult = (Map<String, Object>) responseMap.get("result");
            String resultCode = omPayResult != null ? (String) omPayResult.get("code") : null;
            String resultDescription = omPayResult != null ? (String) omPayResult.get("description") : "Capture Processed";

            PaymentPluginStatus status = mapOmpayStatusToKillBill(ompayState);
            if (!response.isSuccess() && status != PaymentPluginStatus.PROCESSED) {
                status = PaymentPluginStatus.ERROR;
            }


            dao.addResponse(kbAccountId, kbPaymentId, kbTransactionId, TransactionType.CAPTURE, amount, currency,
                    newOmPayTxnId, originalAuthOmPayTxnId,
                    null, null, null, null,
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
                        dao.updateResponseAdditionalData(record.getRecordId(),
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
                    null, null, null, null,
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
                    null, null, null, null,
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
        logger.info("getPaymentInfo called for kbPaymentId: {}", kbPaymentId);

        boolean refreshFromGateway = false;
        if (properties != null) {
            for (PluginProperty prop : properties) {
                // Allowing Kill Bill to explicitly request a refresh via a property
                if ("REQUEST_PLUGIN_REFRESH".equals(prop.getKey()) && "true".equalsIgnoreCase(String.valueOf(prop.getValue()))) {
                    refreshFromGateway = true;
                    break;
                }
            }
        }

        if (refreshFromGateway) {
            logger.info("Refresh requested for payment info for kbPaymentId: {}", kbPaymentId);
            try {
                OmpayResponsesRecord primaryRecord = dao.getLatestSuccessfulTransaction(kbPaymentId, context.getTenantId());

                if (primaryRecord != null && !Strings.isNullOrEmpty(primaryRecord.getOmpayTransactionId())) {
                    String ompayReferenceId = primaryRecord.getOmpayTransactionId();
                    OmPayConfigProperties config = configurationHandler.getConfigurable(context.getTenantId());
                    String retrieveUrl = config.getApiBaseUrlWithMerchant() + "/payment/" + ompayReferenceId;

                    logger.debug("Calling OMPay to retrieve payment details: {}", retrieveUrl);
                    OmPayHttpClient.OmPayHttpResponse response = httpClient.doGet(retrieveUrl, config.getBasicAuthHeader());
                    Map<String, Object> gatewayResponseMap = response.getResponseMap();

                    if (response.isSuccess() && gatewayResponseMap != null) {
                        // Update your local ompay_responses table with the fresh data.
                        // The gatewayResponseMap is the full, fresh response for this OMPay transaction.
                        dao.updateResponseByOmPayTxnId(ompayReferenceId, gatewayResponseMap, context.getTenantId());
                        logger.info("Successfully refreshed payment info for OMPay ID {} from gateway.", ompayReferenceId);

                        // If the refresh changes the status of a PENDING transaction, notify Kill Bill
                        String newOmpayState = (String) gatewayResponseMap.get("state");
                        if (newOmpayState != null) {
                            OmpayResponsesRecord updatedRecord = dao.getResponseByOmPayTransactionId(ompayReferenceId, context.getTenantId());
                            if (updatedRecord != null) {
                                PluginPaymentTransactionInfoPlugin currentLocalInfo = dao.toPaymentTransactionInfoPlugin(updatedRecord);
                                PaymentPluginStatus newPluginStatus = mapOmpayStatusToKillBill(newOmpayState);

                                if (currentLocalInfo.getStatus() == PaymentPluginStatus.PENDING && currentLocalInfo.getStatus() != newPluginStatus) {
                                    logger.info("Payment {} (OMPay ID {}) status changed from PENDING to {} after gateway refresh. Notifying Kill Bill.",
                                            updatedRecord.getKbPaymentTransactionId(), ompayReferenceId, newPluginStatus);
                                    Account account = killbillAPI.getAccountUserApi().getAccountById(kbAccountId, context);
                                    killbillAPI.getPaymentApi().notifyPendingTransactionOfStateChanged(
                                            account,
                                            UUID.fromString(updatedRecord.getKbPaymentTransactionId()),
                                            (newPluginStatus == PaymentPluginStatus.PROCESSED),
                                            (CallContext) context // Cast if context is CallContext, otherwise create one
                                    );
                                }
                            }
                        }

                    } else {
                        logger.warn("Failed to refresh payment info for OMPay ID {} from gateway. Status: {}, Body: {}",
                                ompayReferenceId, response.getStatusCode(), response.getResponseBody());
                        // Potentially throw an error if refresh is critical, or just log and return local data.
                        // For now, we'll fall through and return possibly stale local data.
                    }
                } else {
                    logger.warn("Could not find a suitable primary OMPay transaction ID for kbPaymentId {} to refresh from gateway.", kbPaymentId);
                }
            } catch (PaymentPluginApiException e) { // Catch plugin specific exceptions first
                throw e;
            }
            catch (Exception e) { // SQLException, AccountApiException, HTTP client exception, etc.
                logger.error("Error during gateway refresh for getPaymentInfo (kbPaymentId {}): {}", kbPaymentId, e.getMessage(), e);
                // Fall through to return local data, or throw new PaymentPluginApiException
                // For robustness, we might choose to return local data rather than failing the whole call.
                // throw new PaymentPluginApiException("Gateway Refresh Error", "Failed to refresh payment data from OMPay: " + e.getMessage());
            }
        }

        // Always return all known (local) transactions for the payment
        try {
            return dao.getPaymentInfosForKbPaymentId(kbPaymentId, context.getTenantId());
        } catch (SQLException e) {
            logger.error("Error retrieving payment info from DB for kbPaymentId {}: {}", kbPaymentId, e.getMessage());
            throw new PaymentPluginApiException("DB Error", "Could not retrieve payment info: " + e.getMessage());
        }
    }

    // In OmPayPaymentPluginApi.java

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
                            logger.debug("Received {} cards from OMPay for payer {}", ompayCards.size(), ompayPayerId);
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
            List<OmpayPaymentMethodsRecord> records = dao.getPaymentMethods(kbAccountId, context.getTenantId()); // This calls super.getPaymentMethods
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