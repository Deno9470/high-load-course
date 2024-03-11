package ru.quipy.payments.subscribers

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.orders.api.OrderAggregate
import ru.quipy.orders.api.OrderPaymentStartedEvent
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.ExternalServicesConfig
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.PaymentManager
import ru.quipy.payments.logic.PaymentService
import ru.quipy.payments.logic.create
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import java.util.concurrent.Executors
import javax.annotation.PostConstruct

@Service
class OrderPaymentSubscriber {

    val logger: Logger = LoggerFactory.getLogger(OrderPaymentSubscriber::class.java)

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    @Qualifier(ExternalServicesConfig.PRIMARY_PAYMENT_BEAN)
    private lateinit var paymentService: PaymentService
    @Autowired
    @Qualifier(ExternalServicesConfig.FIRST_SERVICE_BEAN)
    private lateinit var firstPaymentService: PaymentService
    @Autowired
    @Qualifier(ExternalServicesConfig.SECOND_SERVICE_BEAN)
    private lateinit var secondPaymentService: PaymentService

    private var paymentManager = PaymentManager(listOf(firstPaymentService, secondPaymentService))

    private val paymentExecutor = Executors.newFixedThreadPool(16, NamedThreadFactory("payment-executor"))

    @PostConstruct
    fun init() {
        // TODO Это менят надо
        // Тут надо чекать второй акк, если он не может, то надо тыкать первый
        // Сделать счетчик транзакций и перенаправлять лишние
        subscriptionsManager.createSubscriber(OrderAggregate::class, "payments:order-subscriber", retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)) {
            `when`(OrderPaymentStartedEvent::class) { event ->
                paymentExecutor.submit {
                    val createdEvent = paymentESService.create {
                        it.create(
                            event.paymentId,
                            event.orderId,
                            event.amount
                        )
                    }
                    logger.info("Payment ${createdEvent.paymentId} for order ${event.orderId} created.")

                    paymentManager.submitPaymentRequest(createdEvent.paymentId, event.amount, event.createdAt)
                }
            }
        }
    }
}