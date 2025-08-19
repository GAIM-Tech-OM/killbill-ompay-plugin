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
package org.killbill.billing.plugin.ompay.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class OmPayHttpClient {

    private static final Logger logger = LoggerFactory.getLogger(OmPayHttpClient.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final int DEFAULT_CONNECT_TIMEOUT = 10000; // 10 seconds
    private static final int DEFAULT_READ_TIMEOUT = 30000; // 30 seconds

    public static class OmPayHttpResponse {
        private final int statusCode;
        private final String responseBody;
        private final Map<String, Object> responseMap;

        @SuppressWarnings("unchecked")
        public OmPayHttpResponse(int statusCode, String responseBody, ObjectMapper mapper) {
            this.statusCode = statusCode;
            this.responseBody = responseBody;
            Map<String, Object> tempMap = null;
            if (responseBody != null && !responseBody.isEmpty()) {
                try {
                    // Check if it's "DONE" which is not valid JSON
                    if ("\"DONE\"".equalsIgnoreCase(responseBody.trim())) {
                        tempMap = Map.of("status", "DONE");
                    } else {
                        tempMap = mapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
                    }
                } catch (Exception e) {
                    logger.warn("Could not parse JSON response: {} - Error: {}", responseBody, e.getMessage());
                    // If parsing fails, but it's a success code, maybe body is not JSON (e.g. "DONE" string)
                    if (statusCode >= 200 && statusCode < 300 && !responseBody.startsWith("{") && !responseBody.startsWith("[")) {
                        tempMap = Map.of("rawResponse", responseBody);
                    }
                }
            }
            this.responseMap = tempMap;
        }

        public int getStatusCode() { return statusCode; }
        public String getResponseBody() { return responseBody; }
        @Nullable public Map<String, Object> getResponseMap() { return responseMap; }
        public boolean isSuccess() { return statusCode >= 200 && statusCode < 300; }
    }

    private OmPayHttpResponse performRequest(final String urlString,
                                             final String method,
                                             @Nullable final String body,
                                             @Nullable final String authorizationHeader,
                                             @Nullable final String contentType) throws Exception {
        HttpURLConnection conn = null;
        try {
            final URL url = new URL(urlString);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(method);
            conn.setConnectTimeout(DEFAULT_CONNECT_TIMEOUT);
            conn.setReadTimeout(DEFAULT_READ_TIMEOUT);

            if (authorizationHeader != null) {
                conn.setRequestProperty("Authorization", authorizationHeader);
            }
            conn.setRequestProperty("Accept", "application/json");

            if ("POST".equals(method) || "PUT".equals(method)) {
                conn.setDoOutput(true);
                if (contentType != null) {
                    conn.setRequestProperty("Content-Type", contentType);
                } else {
                    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                }

                if (body != null && !body.isEmpty()) {
                    try (OutputStream os = conn.getOutputStream()) {
                        byte[] input = body.getBytes(StandardCharsets.UTF_8);
                        os.write(input, 0, input.length);
                    }
                } else if ("POST".equals(method)) { // Special case for client_token POST with empty body
                    conn.setFixedLengthStreamingMode(0);
                }
            }

            final int statusCode = conn.getResponseCode();
            final StringBuilder response = new StringBuilder();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    (statusCode >= 200 && statusCode < 300 && conn.getInputStream() != null) ? conn.getInputStream() : conn.getErrorStream(), StandardCharsets.UTF_8))) {
                String responseLine;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
            } catch (IOException e) {
                if (statusCode >= 200 && statusCode < 300 && response.length() == 0) {
                    logger.debug("No content in response body for status code: {}", statusCode);
                } else if (response.length() == 0) {
                    logger.warn("Could not read input/error stream for status code: {}. Request URL: {}", statusCode, urlString, e);
                }
            }


            logger.info("OMPay API Request to {}: Method={}, Status={}, Response={}", urlString, method, statusCode, response.toString());
            return new OmPayHttpResponse(statusCode, response.toString(), objectMapper);

        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    public OmPayHttpResponse doPost(final String urlString, final String body, @Nullable final String authorizationHeader, @Nullable final String contentType) throws Exception {
        return performRequest(urlString, "POST", body, authorizationHeader, contentType);
    }

    public OmPayHttpResponse doGet(final String urlString, @Nullable final String authorizationHeader) throws Exception {
        return performRequest(urlString, "GET", null, authorizationHeader, null);
    }

    public OmPayHttpResponse doPut(final String urlString, final String body, @Nullable final String authorizationHeader, @Nullable final String contentType) throws Exception {
        return performRequest(urlString, "PUT", body, authorizationHeader, contentType);
    }

    public OmPayHttpResponse doDelete(final String urlString, @Nullable final String authorizationHeader) throws Exception {
        return performRequest(urlString, "DELETE", null, authorizationHeader, null);
    }
}