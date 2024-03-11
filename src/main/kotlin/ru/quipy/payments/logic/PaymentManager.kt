package ru.quipy.payments.logic

import java.util.*

class PaymentManager (
        private val paymentServices: List<PaymentService>
) : PaymentExternalService {

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {

    }
}