-- For PostgreSQL, TIMESTAMP WITHOUT TIME ZONE is equivalent to MySQL's DATETIME
CREATE DOMAIN datetime AS TIMESTAMP WITHOUT TIME ZONE;
CREATE DOMAIN longtext AS TEXT;

CREATE TABLE ompay_responses (
    record_id SERIAL PRIMARY KEY,
    kb_account_id CHAR(36) NOT NULL,
    kb_payment_id CHAR(36) NOT NULL,
    kb_payment_transaction_id CHAR(36) NOT NULL,
    transaction_type VARCHAR(32) NOT NULL,
    amount NUMERIC(15,9),
    currency CHAR(3),
    ompay_transaction_id VARCHAR(255) DEFAULT NULL, -- e.g., "NXWD2R715Z24XB1V80JZ" from OMpay response 'id'
    ompay_reference_id VARCHAR(255) DEFAULT NULL, -- OMpay response 'reference_id'
    ompay_payer_id VARCHAR(255) DEFAULT NULL, -- Store payer.payer_info.id
    ompay_card_id VARCHAR(255) DEFAULT NULL, -- Store payer.funding_instrument.credit_card.id (available after successful auth/payment)
    redirect_url TEXT DEFAULT NULL, -- Store result.redirect_url for OTP
    authenticate_url TEXT DEFAULT NULL, -- Store result.authenticate_url for OTP
    additional_data LONGTEXT DEFAULT NULL, -- To store the full JSON response, including state, result.code, result.description etc.
    ompay_state VARCHAR(32) DEFAULT NULL,
    created_date DATETIME NOT NULL,
    kb_tenant_id CHAR(36) NOT NULL
);
CREATE INDEX idx_ompay_responses_status ON ompay_responses(ompay_state);
CREATE INDEX ompay_responses_kb_payment_id ON ompay_responses(kb_payment_id);
CREATE INDEX ompay_responses_kb_payment_transaction_id ON ompay_responses(kb_payment_transaction_id);
CREATE INDEX ompay_responses_ompay_transaction_id ON ompay_responses(ompay_transaction_id);
CREATE INDEX ompay_responses_ompay_reference_id ON ompay_responses(ompay_reference_id);


CREATE TABLE ompay_payment_methods (
    record_id SERIAL PRIMARY KEY,
    kb_account_id CHAR(36) NOT NULL,
    kb_payment_method_id CHAR(36) NOT NULL,
    ompay_payer_id VARCHAR(255) DEFAULT NULL,
    ompay_credit_card_id VARCHAR(255) NOT NULL, -- This will store the payer.funding_instrument.credit_card.id (tokenized card from OMpay vault)
    is_default SMALLINT NOT NULL DEFAULT 0,
    is_deleted SMALLINT NOT NULL DEFAULT 0,
    additional_data LONGTEXT DEFAULT NULL, -- To store card details like last4, expiry, brand, etc.
    created_date DATETIME NOT NULL,
    updated_date DATETIME NOT NULL,
    kb_tenant_id CHAR(36) NOT NULL
);
CREATE UNIQUE INDEX ompay_payment_methods_kb_payment_method_id ON ompay_payment_methods(kb_payment_method_id);
CREATE INDEX ompay_payment_methods_ompay_credit_card_id ON ompay_payment_methods(ompay_credit_card_id);