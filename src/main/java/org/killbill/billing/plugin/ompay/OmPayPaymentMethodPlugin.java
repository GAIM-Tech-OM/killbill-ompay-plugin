package org.killbill.billing.plugin.ompay;

import java.util.List;
import java.util.UUID;

import org.killbill.billing.payment.api.PluginProperty;
import org.killbill.billing.plugin.api.payment.PluginPaymentMethodPlugin;

public class OmPayPaymentMethodPlugin extends PluginPaymentMethodPlugin {

    public OmPayPaymentMethodPlugin(UUID kbPaymentMethodId,
                                    String externalPaymentMethodId,
                                    boolean isDefaultPaymentMethod,
                                    List<PluginProperty> properties) {
        super(kbPaymentMethodId,
                externalPaymentMethodId,
                isDefaultPaymentMethod,
                properties);
    }
}