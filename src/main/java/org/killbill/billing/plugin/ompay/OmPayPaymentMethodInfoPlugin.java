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

import java.util.UUID;
import org.killbill.billing.plugin.api.payment.PluginPaymentMethodInfoPlugin;

public class OmPayPaymentMethodInfoPlugin extends PluginPaymentMethodInfoPlugin {

    public OmPayPaymentMethodInfoPlugin(final UUID kbAccountId,
                                        final UUID kbPaymentMethodId,
                                        final boolean isDefault,
                                        final String externalPaymentMethodId) {
        super(kbAccountId,
                kbPaymentMethodId,
                isDefault,
                externalPaymentMethodId);
    }
}