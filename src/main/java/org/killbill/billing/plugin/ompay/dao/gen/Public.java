/*
 * This file is generated by jOOQ.
 */
package org.killbill.billing.plugin.ompay.dao.gen;


import java.util.Arrays;
import java.util.List;

import org.jooq.Catalog;
import org.jooq.Table;
import org.jooq.impl.SchemaImpl;
import org.killbill.billing.plugin.ompay.dao.gen.tables.OmpayPaymentMethods;
import org.killbill.billing.plugin.ompay.dao.gen.tables.OmpayResponses;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Public extends SchemaImpl {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public</code>
     */
    public static final Public PUBLIC = new Public();

    /**
     * The table <code>public.ompay_payment_methods</code>.
     */
    public final OmpayPaymentMethods OMPAY_PAYMENT_METHODS = OmpayPaymentMethods.OMPAY_PAYMENT_METHODS;

    /**
     * The table <code>public.ompay_responses</code>.
     */
    public final OmpayResponses OMPAY_RESPONSES = OmpayResponses.OMPAY_RESPONSES;

    /**
     * No further instances allowed
     */
    private Public() {
        super("public", null);
    }


    @Override
    public Catalog getCatalog() {
        return DefaultCatalog.DEFAULT_CATALOG;
    }

    @Override
    public final List<Table<?>> getTables() {
        return Arrays.asList(
            OmpayPaymentMethods.OMPAY_PAYMENT_METHODS,
            OmpayResponses.OMPAY_RESPONSES
        );
    }
}
