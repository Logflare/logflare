let hooks = {}

hooks.PaymentMethodForm = {
    init(stripeKey, stripeCustomer, hook) {
        let stripe = Stripe(stripeKey);
        let elements = stripe.elements();

        const style = {
            base: {
                iconColor: '#c4f0ff',
                color: '#fff',
                fontWeight: 500,
                fontFamily: 'Roboto, Open Sans, Segoe UI, sans-serif',
                fontSize: '16px',
                fontSmoothing: 'antialiased',
                ':-webkit-autofill': {
                    color: '#fce883',
                },
                '::placeholder': {
                    color: '#87BBFD',
                },
            },
            invalid: {
                iconColor: '#FFC7EE',
                color: '#FFC7EE',
            }
        };

        let card = elements.create('card', { style: style });

        card.mount('#card-element');

        card.on('change', function (event) {
            displayError(event);
        });

        var form = document.getElementById('payment-form');

        form.addEventListener('submit', function (event) {
            createPaymentMethod(card, stripeCustomer, 'price_1HaODjLvvReWx3FxJLROGB9I')
        });

        function displayError(event) {
            let displayError = document.getElementById('card-element-errors');
            if (event.error) {

                /*
                hook.pushEvent("payment-method-error", {
                    message: event.error.message
                })
                */
                displayError.textContent = event.error.message
            } else {
                /*
                hook.pushEvent("clear-flash", {

                })
                */
                displayError.textContent = ''
            }
        };

        function createPaymentMethod(cardElement, customerId, priceId) {
            return stripe
                .createPaymentMethod({
                    type: 'card',
                    card: cardElement,
                })
                .then((result) => {
                    if (result.error) {
                        displayError(result);
                    } else {
                        hook.pushEvent("save-payment-and-subscribe", {
                            id: result.paymentMethod.id,
                            customer_id: customerId,
                            price_id: priceId
                        })
                    }
                });
        };
    },

    mounted() {
        var el = this.el
        var hook = this
        this.init(el.dataset.stripeKey, el.dataset.stripeCustomer, hook)
        console.log("mounted")

    },

    updated() {
        var el = this.el
        var hook = this
        this.init(el.dataset.stripeKey, el.dataset.stripeCustomer, hook)
        console.log("updated")
    },

    disconnected() {
        console.log("disconnected")
    },

    reconnected() {
        console.log("reconnected")
    },

    destroyed() {
        console.log("destroyed")
    }

}

export default hooks