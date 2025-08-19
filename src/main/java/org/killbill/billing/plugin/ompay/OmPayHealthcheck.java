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

import java.util.Map;

import javax.annotation.Nullable;

import org.killbill.billing.osgi.api.Healthcheck;
import org.killbill.billing.tenant.api.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OmPayHealthcheck implements Healthcheck {

    private static final Logger logger = LoggerFactory.getLogger(OmPayHealthcheck.class);

    private final OmPayConfigurationHandler configurationHandler;

    public OmPayHealthcheck(final OmPayConfigurationHandler configurationHandler) {
        this.configurationHandler = configurationHandler;
    }

    @Override
    public HealthStatus getHealthStatus(@Nullable final Tenant tenant, @Nullable final Map properties) {
        if (tenant == null) {
            // The plugin is running
            return HealthStatus.healthy("OmPay plugin is running");
        } else {
            // Check tenant-specific configuration
            final OmPayConfigProperties config = configurationHandler.getConfigurable(tenant.getId());
            if (config.getMerchantId() == null || config.getApiBaseUrl() == null || config.getBasicAuthHeader() == null) {
                return HealthStatus.unHealthy("OmPay plugin misconfigured for tenant: " + tenant.getId() +
                        ". Missing merchantId, apiBaseUrl, or clientId/clientSecret.");
            }

            // TODO: Implement a lightweight API call to OMpay to check connectivity and authentication
            // For example:
            // try {
            //     final String healthCheckUrl = config.getApiBaseUrl() + "/some_status_endpoint"; // Replace with actual endpoint
            //     final HttpClient httpClient = new HttpClient(healthCheckUrl, config.getBasicAuthHeader(), null, null, null, true, 6000, 10000);
            //     Response response = httpClient.doGet(null, null);
            //     if (response.getStatusCode() >= 200 && response.getStatusCode() < 300) {
            //         return HealthStatus.healthy("OmPay API connection OK for tenant: " + tenant.getId());
            //     } else {
            //         return HealthStatus.unHealthy("OmPay API connection failed for tenant: " + tenant.getId() +
            //                                       ". Status: " + response.getStatusCode() + ", Body: " + response.getResponseBody());
            //     }
            // } catch (Exception e) {
            //     logger.warn("OmPay Healthcheck failed for tenant {}: ", tenant.getId(), e);
            //     return HealthStatus.unHealthy("OmPay API connection failed for tenant: " + tenant.getId() + ". Error: " + e.getMessage());
            // }

            return HealthStatus.healthy("OmPay plugin configured for tenant: " + tenant.getId() + ". API Ping not yet implemented.");
        }
    }
}