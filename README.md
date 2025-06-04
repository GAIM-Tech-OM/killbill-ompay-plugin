# killbill-ompay-plugin

Kill Bill payment plugin that uses [OMPay](https://docs.ompay.com/) as the payment gateway.

## Configuration

Configure the plugin with your OMPay merchant details and API credentials. The available properties are:

* `org.killbill.billing.plugin.ompay.merchantId`: Your OMPay Merchant ID.
* `org.killbill.billing.plugin.ompay.clientId`: Your OMPay API Client ID.
* `org.killbill.billing.plugin.ompay.clientSecret`: Your OMPay API Client Secret.
* `org.killbill.billing.plugin.ompay.testMode`: Set to `true` for sandbox/testing, `false` for live. Defaults to `true`.
* `org.killbill.billing.plugin.ompay.apiBaseUrl`: (Optional) Override the default OMPay API base URL. Defaults are `https://api.sandbox.ompay.com/v1/merchants` for test mode and `https://api.ompay.com/v1/merchants` for live mode.
* `org.killbill.billing.plugin.ompay.killbillBaseUrl`: (Optional) The base URL of your Kill Bill instance, used for constructing redirect URLs. Defaults to `http://127.0.0.1:8080`.

Upload the configuration to Kill Bill for your tenant:

```bash
curl -v \
     -X POST \
     -u admin:password \
     -H 'X-Killbill-ApiKey: bob' \
     -H 'X-Killbill-ApiSecret: lazar' \
     -H 'X-Killbill-CreatedBy: admin' \
     -H 'Content-Type: text/plain' \
     -d 'org.killbill.billing.plugin.ompay.merchantId=YOUR_OMPAY_MERCHANT_ID
org.killbill.billing.plugin.ompay.clientId=YOUR_OMPAY_CLIENT_ID
org.killbill.billing.plugin.ompay.clientSecret=YOUR_OMPAY_CLIENT_SECRET
org.killbill.billing.plugin.ompay.testMode=true # Optional
# org.killbill.billing.plugin.ompay.killbillBaseUrl=http://<your_killbill_host>:8080 # Optional
# org.killbill.billing.plugin.ompay.apiBaseUrl=[https://custom.api.ompay.com/v1/merchants](https://custom.api.ompay.com/v1/merchants) # Optional' \
     [http://127.0.0.1:8080/1.0/kb/tenants/uploadPluginConfig/killbill-ompay](http://127.0.0.1:8080/1.0/kb/tenants/uploadPluginConfig/killbill-ompay)
```

## Testing / Payment Flow

This outlines the typical flow for adding a payment method using OMPay, potentially involving 3DS.

1.  **Create a Kill Bill account for the customer:**
    (Standard Kill Bill API - same as your Gocardless example)
    This will return the Kill Bill `accountId`.

    ```bash
    curl -v \
         -X POST \
         -u admin:password \
         -H 'X-Killbill-ApiKey: bob' \
         -H 'X-Killbill-ApiSecret: lazar' \
         -H 'X-Killbill-CreatedBy: tutorial' \
         -H 'Content-Type: application/json' \
         -d '{ "currency": "OMR" }' \
         '[http://127.0.0.1:8080/1.0/kb/accounts](http://127.0.0.1:8080/1.0/kb/accounts)'
    ```
    Note the `accountId` from the `Location` header in the response.

2.  **Get Payment Form Descriptor from OMPay Plugin:**
    Your application backend calls the plugin's `/form` endpoint to get details needed to render the OMPay payment form.

    ```bash
    curl -v \
         -X GET \
         -u admin:password \
         -H "X-Killbill-ApiKey: bob" \
         -H "X-Killbill-ApiSecret: lazar" \
         -H 'X-Killbill-CreatedBy: tutorial' \
         '[http://127.0.0.1:8080/plugins/killbill-ompay/form?kbAccountId=](http://127.0.0.1:8080/plugins/killbill-ompay/form?kbAccountId=)<ACCOUNT_ID>&returnUrl=[https://yourshop.com/payment/ompay_return&cancelUrl=https://yourshop.com/payment/ompay_cancel&paymentIntent=auth&currency=OMR&amount=0.100](https://yourshop.com/payment/ompay_return&cancelUrl=https://yourshop.com/payment/ompay_cancel&paymentIntent=auth&currency=OMR&amount=0.100)'
    ```
    * Replace `<ACCOUNT_ID>` with the customer's Kill Bill account ID.
    * `returnUrl`: URL where OMPay redirects after successful 3DS or payment attempt.
    * `cancelUrl`: URL where OMPay redirects if the user cancels.
    * `paymentIntent`: `auth` for authorization, `sale` for purchase.
    * `currency` & `amount`: Specify for the transaction (e.g., a small auth amount).

    The response will be a JSON similar to this:
    ```json
    {
        "kbAccountId": "<ACCOUNT_ID>",
        "formMethod": "POST",
        "formUrl": "[http://127.0.0.1:8080/plugins/killbill-ompay/process-nonce](http://127.0.0.1:8080/plugins/killbill-ompay/process-nonce)",
        "formFields": [
            {"key": "amount", "value": "0.100", "isHidden": false},
            {"key": "currency", "value": "OMR", "isHidden": false}
        ],
        "properties": [
            {"key": "ompayClientToken", "value": "your_ompay_client_access_token", "isHidden": false},
            {"key": "isSandbox", "value": "true", "isHidden": false}
        ]
    }
    ```
    Key fields:
    * `formUrl`: The endpoint where the payment nonce will be submitted.
    * `properties.ompayClientToken`: The client token from OMPay to initialize their SDK/form.

3.  **Render OMPay Form and Submit Nonce:**
    * On your frontend, use the `ompayClientToken` to initialize the OMPay payment form (e.g., using OMPay's JavaScript SDK).
    * When the user submits their card details, the OMPay SDK will provide a payment `nonce`.
    * Your application backend then POSTs this `nonce` and other details to the `formUrl` obtained in Step 2 (which is `/plugins/killbill-ompay/process-nonce`).

    ```bash
    curl -v \
         -X POST \
         -u admin:password \
         -H "X-Killbill-ApiKey: bob" \
         -H "X-Killbill-ApiSecret: lazar" \
         -H 'X-Killbill-CreatedBy: tutorial' \
         -H "Content-Type: application/x-www-form-urlencoded" \
         -d "nonce=<GENERATED_OMPAY_NONCE>&kbAccountId=<ACCOUNT_ID>&amount=0.100&currency=OMR&paymentIntent=auth&returnUrl=[https://yourshop.com/payment/ompay_return&cancelUrl=https://yourshop.com/payment/ompay_cancel&force3ds=false](https://yourshop.com/payment/ompay_return&cancelUrl=https://yourshop.com/payment/ompay_cancel&force3ds=false)" \
         '[http://127.0.0.1:8080/plugins/killbill-ompay/process-nonce](http://127.0.0.1:8080/plugins/killbill-ompay/process-nonce)'
    ```
    The response from `/process-nonce` will be JSON. Example:
    ```json
    {
        "success": true,
        "kb_payment_id": "...",
        "kb_transaction_id": "...",
        "transaction_type": "AUTHORIZE",
        "status": "PENDING", // or PROCESSED, ERROR
        "ompay_transaction_id": "ompay_txn_...",
        "requires_3ds": true, // if true, a redirect_url will be present
        "redirect_url": "https://ompay_3ds_url/..." // if requires_3ds is true
    }
    ```

4.  **Handle 3DS Redirect (if applicable):**
    * If the response from Step 3 indicates `requires_3ds: true` and provides a `redirect_url`, your application must redirect the user's browser to this OMPay URL.
    * After the user completes the 3DS authentication, OMPay will redirect them back to the `returnUrl` you initially provided. This callback to your `returnUrl` should include a `sessionId` from OMPay (e.g., as a query parameter `?sessionId=ompay_session_xyz`).

5.  **Add Payment Method to Kill Bill (Complete 3DS Flow or if no 3DS):**
    * **If 3DS occurred:** Extract the `sessionId` from the OMPay redirect (Step 4).
    * Call Kill Bill's standard endpoint to add a payment method. Pass the `sessionId` as a plugin property. This tells the OMPay plugin to finalize the payment method addition using the session data from OMPay.

        ```bash
        curl -v \
             -X POST \
             -u admin:password \
             -H 'X-Killbill-ApiKey: bob' \
             -H 'X-Killbill-ApiSecret: lazar' \
             -H 'X-Killbill-CreatedBy: tutorial' \
             -H 'Content-Type: application/json' \
             -d '{
               "pluginName": "killbill-ompay",
               "pluginInfo": {
                 "properties": [
                   {
                     "key": "sessionId",
                     "value": "<SESSION_ID_FROM_OMPAY_REDIRECT>"
                   }
                   // You might also include other properties if needed by your frontend/backend logic
                 ]
               }
             }' \
             '[http://127.0.0.1:8080/1.0/kb/accounts/](http://127.0.0.1:8080/1.0/kb/accounts/)<ACCOUNT_ID>/paymentMethods?isDefault=true'
        ```
    * **If no 3DS was required (Step 3 resulted in `status: "PROCESSED"` and `requires_3ds: false`):** The payment method might have been added automatically during the `/process-nonce` call if the transaction was successful and yielded a card token. You can verify by listing payment methods for the account. If it wasn't automatically added, or if you prefer explicit control, you could potentially call the same endpoint as above but without the `sessionId` and instead providing OMPay card/payer tokens if available and if the plugin supports direct token-based PM creation (check `OmPayPaymentPluginApi#addPaymentMethodDirect` logic). However, the primary flow described involves the nonce and optional session.

    This call (with `sessionId`) returns the Kill Bill `paymentMethodId` in the `Location` header.

6.  **Trigger Payments against the new Payment Method:**
    (Standard Kill Bill API - same as your Gocardless example, using the `paymentMethodId` from Step 5)

    ```bash
    curl -v \
         -X POST \
         -u admin:password \
         -H "X-Killbill-ApiKey: bob" \
         -H "X-Killbill-ApiSecret: lazar" \
         -H "X-Killbill-CreatedBy: tutorial" \
         -H "Content-Type: application/json" \
         --data-binary '{"transactionType":"PURCHASE","amount":"10.000", "currency": "OMR", "paymentMethodId": "<PAYMENT_METHOD_ID>"}' \
         '[http://127.0.0.1:8080/1.0/kb/accounts/](http://127.0.0.1:8080/1.0/kb/accounts/)<ACCOUNT_ID>/payments'
    ```
    Note the `paymentId` from the `Location` header.

7.  **Obtain Information about the Payment:**
    (Standard Kill Bill API - same as your Gocardless example)

    ```bash
    curl -v \
         -u admin:password \
         -H "X-Killbill-ApiKey: bob" \
         -H "X-Killbill-ApiSecret: lazar" \
         '[http://127.0.0.1:8080/1.0/kb/payments/](http://127.0.0.1:8080/1.0/kb/payments/)<PAYMENT_ID>?withPluginInfo=true'
    ```

8.  **Webhook Handling:**
    Configure OMPay to send webhooks to `/plugins/killbill-ompay/webhook`. The plugin will process these notifications to update transaction statuses in Kill Bill.

This flow should align with how your `OmPayPaymentPluginApi.java`, `OmPayFormServlet.java`, and `OmPayNonceHandlerServlet.java` are structured and how your C# `BillingService` interacts with them.