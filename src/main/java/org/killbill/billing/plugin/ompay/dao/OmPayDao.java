package org.killbill.billing.plugin.ompay.dao;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.joda.time.DateTime;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL; // Import for DSL.using
import org.killbill.billing.catalog.api.Currency;
import org.killbill.billing.payment.api.PluginProperty; // Ensure this is the correct import
import org.killbill.billing.payment.api.TransactionType;
import org.killbill.billing.payment.plugin.api.PaymentTransactionInfoPlugin;
import org.killbill.billing.plugin.api.payment.PluginPaymentTransactionInfoPlugin;
import org.killbill.billing.plugin.dao.payment.PluginPaymentDao; // Correct base class
import org.killbill.billing.plugin.ompay.dao.gen.tables.OmpayPaymentMethods;
import org.killbill.billing.plugin.ompay.dao.gen.tables.OmpayResponses;
import org.killbill.billing.plugin.ompay.dao.gen.tables.records.OmpayPaymentMethodsRecord;
import org.killbill.billing.plugin.ompay.dao.gen.tables.records.OmpayResponsesRecord;
import org.killbill.billing.payment.plugin.api.PaymentPluginStatus;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection; // Import for Connection
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.killbill.billing.plugin.ompay.dao.gen.Tables.OMPAY_PAYMENT_METHODS;
import static org.killbill.billing.plugin.ompay.dao.gen.Tables.OMPAY_RESPONSES;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableList; // For empty lists

public class OmPayDao extends PluginPaymentDao<
        OmpayResponsesRecord, OmpayResponses,
        OmpayPaymentMethodsRecord, OmpayPaymentMethods
        > {
    private static final Logger logger = LoggerFactory.getLogger(OmPayDao.class);

    public OmPayDao(final DataSource dataSource) throws SQLException {
        super(OMPAY_RESPONSES, OMPAY_PAYMENT_METHODS, dataSource);
        // objectMapper is already initialized in PluginDao, but if you need specific config for it here:
        // this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        // However, PluginDao.objectMapper is protected, so you can use it directly.
    }

    public void addResponse(final UUID kbAccountId,
                            final UUID kbPaymentId,
                            final UUID kbTransactionId,
                            final TransactionType transactionType,
                            @Nullable final BigDecimal amount,
                            @Nullable final Currency currency,
                            final String ompayTransactionId,
                            @Nullable final String ompayReferenceId,
                            @Nullable final String ompayPayerId,
                            @Nullable final String ompayCardId,
                            @Nullable final String redirectUrl,
                            @Nullable final String authenticateUrl,
                            final Map<String, Object> additionalDataMap,
                            final DateTime utcNow,
                            final UUID kbTenantId) throws SQLException, JsonProcessingException {

        final String additionalData = objectMapper.writeValueAsString(additionalDataMap);
        final LocalDateTime ldtUtcNow = toLocalDateTime(utcNow); // Use utility from PluginDao

        execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            dslContext.insertInto(OMPAY_RESPONSES,
                            OMPAY_RESPONSES.KB_ACCOUNT_ID,
                            OMPAY_RESPONSES.KB_PAYMENT_ID,
                            OMPAY_RESPONSES.KB_PAYMENT_TRANSACTION_ID,
                            OMPAY_RESPONSES.TRANSACTION_TYPE,
                            OMPAY_RESPONSES.AMOUNT,
                            OMPAY_RESPONSES.CURRENCY,
                            OMPAY_RESPONSES.OMPAY_TRANSACTION_ID,
                            OMPAY_RESPONSES.OMPAY_REFERENCE_ID,
                            OMPAY_RESPONSES.OMPAY_PAYER_ID,
                            OMPAY_RESPONSES.OMPAY_CARD_ID,
                            OMPAY_RESPONSES.REDIRECT_URL,
                            OMPAY_RESPONSES.AUTHENTICATE_URL,
                            OMPAY_RESPONSES.ADDITIONAL_DATA,
                            OMPAY_RESPONSES.CREATED_DATE,
                            OMPAY_RESPONSES.KB_TENANT_ID)
                    .values(kbAccountId.toString(),
                            kbPaymentId.toString(),
                            kbTransactionId.toString(),
                            transactionType.toString(),
                            amount,
                            currency != null ? currency.toString() : null,
                            ompayTransactionId,
                            ompayReferenceId,
                            ompayPayerId,
                            ompayCardId,
                            redirectUrl,
                            authenticateUrl,
                            additionalData,
                            ldtUtcNow,
                            kbTenantId.toString())
                    .execute();
            return null;
        });
    }
    public void addPaymentMethod(final UUID kbAccountId,
                                 final UUID kbPaymentMethodId,
                                 final String ompayCreditCardId,
                                 @Nullable final String ompayPayerId, // New parameter
                                 final boolean isDefault,
                                 final Map<String, Object> additionalDataMap,
                                 final DateTime utcNow,
                                 final UUID kbTenantId) throws SQLException, JsonProcessingException {
        final String additionalData = objectMapper.writeValueAsString(additionalDataMap);
        final LocalDateTime ldtNow = toLocalDateTime(utcNow);

        execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            dslContext.insertInto(OMPAY_PAYMENT_METHODS,
                            OMPAY_PAYMENT_METHODS.KB_ACCOUNT_ID,
                            OMPAY_PAYMENT_METHODS.KB_PAYMENT_METHOD_ID,
                            OMPAY_PAYMENT_METHODS.OMPAY_CREDIT_CARD_ID,
                            OMPAY_PAYMENT_METHODS.OMPAY_PAYER_ID, // Add the new field
                            OMPAY_PAYMENT_METHODS.IS_DEFAULT,
                            OMPAY_PAYMENT_METHODS.IS_DELETED,
                            OMPAY_PAYMENT_METHODS.ADDITIONAL_DATA,
                            OMPAY_PAYMENT_METHODS.CREATED_DATE,
                            OMPAY_PAYMENT_METHODS.UPDATED_DATE,
                            OMPAY_PAYMENT_METHODS.KB_TENANT_ID)
                    .values(kbAccountId.toString(),
                            kbPaymentMethodId.toString(),
                            ompayCreditCardId,
                            ompayPayerId, // Store the new value
                            (short) (isDefault ? 1 : 0),
                            (short) 0,
                            additionalData,
                            ldtNow,
                            ldtNow,
                            kbTenantId.toString())
                    .execute();
            return null;
        });
    }

    public List<PaymentTransactionInfoPlugin> getPaymentInfosForKbPaymentId(final UUID kbPaymentId, final UUID kbTenantId) throws SQLException {
        return execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            return dslContext.selectFrom(OMPAY_RESPONSES)
                    .where(OMPAY_RESPONSES.KB_PAYMENT_ID.eq(kbPaymentId.toString()))
                    .and(OMPAY_RESPONSES.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .orderBy(OMPAY_RESPONSES.RECORD_ID.asc())
                    .fetch(this::toPaymentTransactionInfoPlugin); // Assuming toPaymentTransactionInfoPlugin is correctly defined
        });
    }

    public List<OmpayResponsesRecord> getResponsesByKbPaymentIdAndType(final UUID kbPaymentId,
                                                                       final TransactionType transactionType,
                                                                       final String ompaySuccessfulState,
                                                                       final UUID kbTenantId) throws SQLException {
        return execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            List<OmpayResponsesRecord> records = dslContext.selectFrom(OMPAY_RESPONSES)
                    .where(OMPAY_RESPONSES.KB_PAYMENT_ID.eq(kbPaymentId.toString()))
                    .and(OMPAY_RESPONSES.TRANSACTION_TYPE.eq(transactionType.toString()))
                    .and(OMPAY_RESPONSES.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .orderBy(OMPAY_RESPONSES.RECORD_ID.desc())
                    .fetch();

            if (ompaySuccessfulState != null && !ompaySuccessfulState.isEmpty()) {
                return records.stream().filter(r -> {
                    try {
                        Map<String, Object> data = objectMapper.readValue(r.getAdditionalData(), new TypeReference<Map<String,Object>>() {});
                        return ompaySuccessfulState.equalsIgnoreCase((String)data.get("state"));
                    } catch (JsonProcessingException e) {
                        logger.warn("Failed to parse additional_data for filtering state on record_id: {}", r.getRecordId(), e);
                        return false;
                    }
                }).collect(Collectors.toList());
            }
            return records;
        });
    }

    public PluginPaymentTransactionInfoPlugin toPaymentTransactionInfoPlugin(final OmpayResponsesRecord record) {
        Map<String, Object> additionalDataFromDb;
        PaymentPluginStatus pluginStatus = PaymentPluginStatus.UNDEFINED;
        String gatewayError = null;
        String gatewayErrorCode = null;
        String firstPaymentRefId = record.getOmpayTransactionId();
        String secondPaymentRefId = record.getOmpayReferenceId();

        try {
            // Use protected objectMapper from PluginDao
            additionalDataFromDb = objectMapper.readValue(record.getAdditionalData(), new TypeReference<Map<String, Object>>() {});
            String ompayState = (String) additionalDataFromDb.get("state");
            pluginStatus = mapOmpayStatusToKillBill(ompayState);

            Map<String, Object> result = (Map<String, Object>) additionalDataFromDb.get("result");
            if (result != null) {
                gatewayError = (String) result.get("description");
                gatewayErrorCode = (String) result.get("code");
            }
        } catch (IOException e) {
            logger.warn("Failed to deserialize additionalData for record_id: {}", record.getRecordId(), e);
            additionalDataFromDb = Map.of("error", "Failed to parse additionalData: " + e.getMessage(), "originalAdditionalData", record.getAdditionalData());
        }

        DateTime createdDate = new DateTime(record.getCreatedDate().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli());

        List<PluginProperty> props = new ArrayList<>();
        if (additionalDataFromDb != null) {
            additionalDataFromDb.forEach((key, value) -> {
                if (value instanceof String || value instanceof Number || value instanceof Boolean) {
                    props.add(new PluginProperty(key, value, false));
                }
            });
        }


        return new PluginPaymentTransactionInfoPlugin.Builder<>()
                .withKbPaymentId(UUID.fromString(record.getKbPaymentId()))
                .withKbTransactionPaymentId(UUID.fromString(record.getKbPaymentTransactionId()))
                .withTransactionType(TransactionType.valueOf(record.getTransactionType()))
                .withAmount(record.getAmount())
                .withCurrency(record.getCurrency() != null ? Currency.valueOf(record.getCurrency()) : null)
                .withStatus(pluginStatus)
                .withGatewayError(gatewayError)
                .withGatewayErrorCode(gatewayErrorCode)
                .withFirstPaymentReferenceId(firstPaymentRefId)
                .withSecondPaymentReferenceId(secondPaymentRefId)
                .withCreatedDate(createdDate)
                .withEffectiveDate(createdDate)
                .withProperties(props) // Use the extracted properties
                .build();
    }

    private PaymentPluginStatus mapOmpayStatusToKillBill(String ompayState) {
        if (ompayState == null) return PaymentPluginStatus.UNDEFINED;
        switch (ompayState.toLowerCase()) {
            case "authorised": case "authorized":
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
                logger.warn("Unknown OMPay payment state received in DAO mapping: {}", ompayState);
                return PaymentPluginStatus.UNDEFINED;
        }
    }

    public OmpayResponsesRecord getResponseByOmPayTransactionId(final String ompayTransactionId, final UUID kbTenantId) throws SQLException {
        return execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            return dslContext.selectFrom(OMPAY_RESPONSES)
                    .where(OMPAY_RESPONSES.OMPAY_TRANSACTION_ID.eq(ompayTransactionId))
                    .and(OMPAY_RESPONSES.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .orderBy(OMPAY_RESPONSES.RECORD_ID.desc())
                    .limit(1)
                    .fetchOne();
        });
    }

    public OmpayPaymentMethodsRecord getPaymentMethodByKbPaymentMethodId(final UUID kbPaymentMethodId, final UUID kbTenantId) throws SQLException {
        return execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            return dslContext.selectFrom(OMPAY_PAYMENT_METHODS)
                    .where(OMPAY_PAYMENT_METHODS.KB_PAYMENT_METHOD_ID.eq(kbPaymentMethodId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.IS_DELETED.eq((short) 0))
                    .fetchOne();
        });
    }

    public void markPaymentMethodAsDeleted(final UUID kbPaymentMethodId, final UUID kbTenantId) throws SQLException {
        final LocalDateTime ldtUtcNow = toLocalDateTime(new DateTime(org.joda.time.DateTimeZone.UTC));
        execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            return dslContext.update(OMPAY_PAYMENT_METHODS)
                    .set(OMPAY_PAYMENT_METHODS.IS_DELETED, (short) 1)
                    .set(OMPAY_PAYMENT_METHODS.UPDATED_DATE, ldtUtcNow)
                    .where(OMPAY_PAYMENT_METHODS.KB_PAYMENT_METHOD_ID.eq(kbPaymentMethodId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .execute();
        });
    }

    public void clearDefault(final UUID kbAccountId, final UUID kbTenantId) throws SQLException {
        final LocalDateTime ldtUtcNow = toLocalDateTime(new DateTime(org.joda.time.DateTimeZone.UTC));
        execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            return dslContext.update(OMPAY_PAYMENT_METHODS)
                    .set(OMPAY_PAYMENT_METHODS.IS_DEFAULT, (short) 0)
                    .set(OMPAY_PAYMENT_METHODS.UPDATED_DATE, ldtUtcNow)
                    .where(OMPAY_PAYMENT_METHODS.KB_ACCOUNT_ID.eq(kbAccountId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.IS_DEFAULT.eq((short)1))
                    .execute();
        });
    }

    public void setDefaultPaymentMethod(final UUID kbPaymentMethodId, final UUID kbTenantId) throws SQLException {
        final LocalDateTime ldtUtcNow = toLocalDateTime(new DateTime(org.joda.time.DateTimeZone.UTC));
        execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            return dslContext.update(OMPAY_PAYMENT_METHODS)
                    .set(OMPAY_PAYMENT_METHODS.IS_DEFAULT, (short) 1)
                    .set(OMPAY_PAYMENT_METHODS.UPDATED_DATE, ldtUtcNow)
                    .where(OMPAY_PAYMENT_METHODS.KB_PAYMENT_METHOD_ID.eq(kbPaymentMethodId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .execute();
        });
    }

    public void updateResponseAdditionalData(Integer recordId, String additionalData) throws SQLException {
        execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            return dslContext.update(OMPAY_RESPONSES)
                    .set(OMPAY_RESPONSES.ADDITIONAL_DATA, additionalData)
                    .where(OMPAY_RESPONSES.RECORD_ID.eq(recordId))
                    .execute();
        });
    }

    // Add to OmPayDao.java

    public OmpayResponsesRecord getLatestSuccessfulTransaction(final UUID kbPaymentId, final UUID kbTenantId) throws SQLException {
        return execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            // Successful states from OMPay might be "authorised", "captured"
            // We need to check the 'state' field within the additional_data JSON,
            // or rely on the status mapping if we had a dedicated status column.
            // This is complex with current schema. A simpler approach is to find the latest PURCHASE or AUTHORIZE.
            // For a more accurate "successful", we'd need to parse JSON in SQL or iterate results.

            // Fetching latest transaction of type PURCHASE or AUTHORIZE
            // and assuming its 'additional_data' reflects a successful OMPay state
            // This is a simplification. A robust solution needs better status querying.
            List<Condition> conditions = new ArrayList<>();
            conditions.add(OMPAY_RESPONSES.KB_PAYMENT_ID.eq(kbPaymentId.toString()));
            conditions.add(OMPAY_RESPONSES.KB_TENANT_ID.eq(kbTenantId.toString()));
            conditions.add(OMPAY_RESPONSES.TRANSACTION_TYPE.in(TransactionType.PURCHASE.toString(), TransactionType.AUTHORIZE.toString()));

            return dslContext.selectFrom(OMPAY_RESPONSES)
                    .where(conditions)
                    .orderBy(OMPAY_RESPONSES.CREATED_DATE.desc(), OMPAY_RESPONSES.RECORD_ID.desc())
                    .limit(1)
                    .fetchOne();
        });
    }

    public void updateResponseData(final String ompayTransactionId, final Map<String, Object> newAdditionalDataMap, final UUID kbTenantId) throws SQLException {
        try {
            final String newAdditionalData = objectMapper.writeValueAsString(newAdditionalDataMap);
            // We assume created_date doesn't change for an existing transaction when refreshing.
            // If status or other specific fields need updating based on newAdditionalDataMap, that logic would go here.
            // For now, just updating the whole additional_data blob.
            execute(dataSource.getConnection(), (Connection conn) -> {
                final DSLContext dslContext = DSL.using(conn, dialect, settings);
                return dslContext.update(OMPAY_RESPONSES)
                        .set(OMPAY_RESPONSES.ADDITIONAL_DATA, newAdditionalData)
                        // Potentially update other fields if they can change and are in newAdditionalDataMap
                        // .set(OMPAY_RESPONSES.OMPAY_STATE_COLUMN, (String) newAdditionalDataMap.get("state")) // If you had such a column
                        .where(OMPAY_RESPONSES.OMPAY_TRANSACTION_ID.eq(ompayTransactionId))
                        .and(OMPAY_RESPONSES.KB_TENANT_ID.eq(kbTenantId.toString()))
                        .execute();
            });
        } catch (JsonProcessingException e) {
            throw new SQLException("Failed to serialize new additional data for OMPay transaction " + ompayTransactionId, e);
        }
    }

    public void updateResponseByOmPayTxnId(final String ompayTransactionId, final Map<String, Object> newAdditionalDataMap, final UUID kbTenantId) throws SQLException {
        try {
            final String newAdditionalData = objectMapper.writeValueAsString(newAdditionalDataMap);
            OmpayResponsesRecord existingRecord = getResponseByOmPayTransactionId(ompayTransactionId, kbTenantId);
            if (existingRecord != null) {
                updateResponseAdditionalData(existingRecord.getRecordId(), newAdditionalData);
            } else {
                logger.warn("Attempted to update response data for non-existent OMPay transaction ID: {}", ompayTransactionId);
            }
        } catch (JsonProcessingException e) {
            throw new SQLException("Failed to serialize new additional data for OMPay transaction " + ompayTransactionId, e);
        }
    }

    @Nullable
    public String getOmpayPayerIdForAccount(final UUID kbAccountId, final UUID kbTenantId) throws SQLException {
        return execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);
            return dslContext.select(OMPAY_PAYMENT_METHODS.OMPAY_PAYER_ID)
                    .from(OMPAY_PAYMENT_METHODS)
                    .where(OMPAY_PAYMENT_METHODS.KB_ACCOUNT_ID.eq(kbAccountId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.OMPAY_PAYER_ID.isNotNull())
                    .and(OMPAY_PAYMENT_METHODS.IS_DELETED.eq((short) 0))
                    .orderBy(OMPAY_PAYMENT_METHODS.CREATED_DATE.desc()) // Get from a recent PM
                    .limit(1)
                    .fetchOne(OMPAY_PAYMENT_METHODS.OMPAY_PAYER_ID);
        });
    }


    public void synchronizePaymentMethods(final UUID kbAccountId,
                                          final String ompayPayerId,
                                          final List<Map<String, Object>> ompayCardsFromGateway,
                                          final UUID kbTenantId,
                                          final DateTime utcNow) throws SQLException {
        final LocalDateTime ldtNow = toLocalDateTime(utcNow);

        execute(dataSource.getConnection(), (Connection conn) -> {
            final DSLContext dslContext = DSL.using(conn, dialect, settings);

            // 1. Get all current local PMs for this kbAccountId that have this ompayPayerId
            List<OmpayPaymentMethodsRecord> localPms = dslContext.selectFrom(OMPAY_PAYMENT_METHODS)
                    .where(OMPAY_PAYMENT_METHODS.KB_ACCOUNT_ID.eq(kbAccountId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.KB_TENANT_ID.eq(kbTenantId.toString()))
                    .and(OMPAY_PAYMENT_METHODS.OMPAY_PAYER_ID.eq(ompayPayerId)) // Important filter
                    .and(OMPAY_PAYMENT_METHODS.IS_DELETED.eq((short) 0))
                    .fetch();

            Map<String, OmpayPaymentMethodsRecord> localPmsByOmPayCardId = localPms.stream()
                    .collect(Collectors.toMap(OmpayPaymentMethodsRecord::getOmpayCreditCardId, pm -> pm));

            // 2. Iterate through cards from OMPay
            for (Map<String, Object> ompayCard : ompayCardsFromGateway) {
                String ompayCardId = (String) ompayCard.get("id");
                if (Strings.isNullOrEmpty(ompayCardId)) {
                    continue;
                }

                Map<String, Object> additionalDataForDb = new HashMap<>();
                additionalDataForDb.put("ompay_card_type", ompayCard.get("type"));
                additionalDataForDb.put("ompay_card_last4", ompayCard.get("last4"));
                additionalDataForDb.put("ompay_card_expire_month", ompayCard.get("expire_month"));
                additionalDataForDb.put("ompay_card_expire_year", ompayCard.get("expire_year"));
                additionalDataForDb.put("ompay_card_name", ompayCard.get("name"));
                // Add bin_data if available and needed
                if (ompayCard.get("bin_data") instanceof Map) {
                    additionalDataForDb.put("ompay_bin_data", ompayCard.get("bin_data"));
                }


                OmpayPaymentMethodsRecord existingLocalPm = localPmsByOmPayCardId.get(ompayCardId);
                boolean isDefaultGateway = Boolean.TRUE.equals(ompayCard.get("is_default"));


                if (existingLocalPm != null) {
                    // Update existing local PM
                    try {
                        dslContext.update(OMPAY_PAYMENT_METHODS)
                                .set(OMPAY_PAYMENT_METHODS.ADDITIONAL_DATA, objectMapper.writeValueAsString(additionalDataForDb))
                                .set(OMPAY_PAYMENT_METHODS.IS_DEFAULT, (short) (isDefaultGateway ? 1 : 0)) // Sync default status
                                .set(OMPAY_PAYMENT_METHODS.UPDATED_DATE, ldtNow)
                                .where(OMPAY_PAYMENT_METHODS.RECORD_ID.eq(existingLocalPm.getRecordId()))
                                .execute();
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    localPmsByOmPayCardId.remove(ompayCardId); // Mark as processed
                } else {
                    // Add as new local PM
                    UUID newKbPaymentMethodId = UUID.randomUUID();
                    try {
                        dslContext.insertInto(OMPAY_PAYMENT_METHODS,
                                        OMPAY_PAYMENT_METHODS.KB_ACCOUNT_ID,
                                        OMPAY_PAYMENT_METHODS.KB_PAYMENT_METHOD_ID,
                                        OMPAY_PAYMENT_METHODS.OMPAY_CREDIT_CARD_ID,
                                        OMPAY_PAYMENT_METHODS.OMPAY_PAYER_ID,
                                        OMPAY_PAYMENT_METHODS.IS_DEFAULT,
                                        OMPAY_PAYMENT_METHODS.IS_DELETED,
                                        OMPAY_PAYMENT_METHODS.ADDITIONAL_DATA,
                                        OMPAY_PAYMENT_METHODS.CREATED_DATE,
                                        OMPAY_PAYMENT_METHODS.UPDATED_DATE,
                                        OMPAY_PAYMENT_METHODS.KB_TENANT_ID)
                                .values(kbAccountId.toString(),
                                        newKbPaymentMethodId.toString(),
                                        ompayCardId,
                                        ompayPayerId,
                                        (short) (isDefaultGateway ? 1 : 0),
                                        (short) 0,
                                        objectMapper.writeValueAsString(additionalDataForDb),
                                        ldtNow,
                                        ldtNow,
                                        kbTenantId.toString())
                                .execute();
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            // 3. Mark local PMs not found in gateway response as deleted (if policy is to mirror gateway)
            for (OmpayPaymentMethodsRecord localPmToRemove : localPmsByOmPayCardId.values()) {
                dslContext.update(OMPAY_PAYMENT_METHODS)
                        .set(OMPAY_PAYMENT_METHODS.IS_DELETED, (short) 1)
                        .set(OMPAY_PAYMENT_METHODS.UPDATED_DATE, ldtNow)
                        .where(OMPAY_PAYMENT_METHODS.RECORD_ID.eq(localPmToRemove.getRecordId()))
                        .execute();
                logger.info("Marked local OMPay payment method (KB PM ID: {}) as deleted because it was not found in gateway refresh for payer ID: {}",
                        localPmToRemove.getKbPaymentMethodId(), ompayPayerId);
            }
            return null;
        });
    }
}