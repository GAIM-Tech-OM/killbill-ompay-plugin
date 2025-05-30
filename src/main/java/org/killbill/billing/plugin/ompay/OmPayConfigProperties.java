package org.killbill.billing.plugin.ompay;

import java.util.Base64;
import java.util.Properties;
import com.google.common.base.Strings;

public class OmPayConfigProperties {

    private static final String PROPERTY_PREFIX = "org.killbill.billing.plugin.ompay.";

    public static final String TEST_API_BASE_URL = "https://api.sandbox.ompay.com/v1/merchants";
    public static final String LIVE_API_BASE_URL = "https://api.ompay.com/v1/merchants";
    public static final String KILLBILL_BASE_URL_PROPERTY = PROPERTY_PREFIX + "killbillBaseUrl";


    private final String merchantId;
    private final boolean testMode;
    private final String clientId;
    private final String clientSecret;
    private final String apiBaseUrl;
    private final String basicAuthHeader;
    private final String killbillBaseUrl; // New property

    public OmPayConfigProperties(final Properties properties, final String region) {
        this.merchantId = properties.getProperty(PROPERTY_PREFIX + "merchantId");
        this.testMode = Boolean.parseBoolean(properties.getProperty(PROPERTY_PREFIX + "testMode", "true"));
        this.clientId = properties.getProperty(PROPERTY_PREFIX + "clientId");
        this.clientSecret = properties.getProperty(PROPERTY_PREFIX + "clientSecret");
        this.killbillBaseUrl = properties.getProperty(KILLBILL_BASE_URL_PROPERTY, "http://127.0.0.1:8080"); // Default if not set


        if (this.testMode) {
            this.apiBaseUrl = properties.getProperty(PROPERTY_PREFIX + "apiBaseUrl", TEST_API_BASE_URL);
        } else {
            this.apiBaseUrl = properties.getProperty(PROPERTY_PREFIX + "apiBaseUrl", LIVE_API_BASE_URL);
        }

        if (!Strings.isNullOrEmpty(this.clientId) && !Strings.isNullOrEmpty(this.clientSecret)) {
            String authString = this.clientId + ":" + this.clientSecret;
            this.basicAuthHeader = "Basic " + Base64.getEncoder().encodeToString(authString.getBytes());
        } else {
            this.basicAuthHeader = null;
        }
    }

    public String getMerchantId() {
        return merchantId;
    }

    public boolean isTestMode() {
        return testMode;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getApiBaseUrl() {
        return apiBaseUrl;
    }

    public String getApiBaseUrlWithMerchant() {
        if (Strings.isNullOrEmpty(merchantId)) {
            return apiBaseUrl;
        }
        return apiBaseUrl + "/" + merchantId;
    }

    public String getBasicAuthHeader() {
        return basicAuthHeader;
    }

    public String getKillbillBaseUrl() {
        return killbillBaseUrl;
    }
}