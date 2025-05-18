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