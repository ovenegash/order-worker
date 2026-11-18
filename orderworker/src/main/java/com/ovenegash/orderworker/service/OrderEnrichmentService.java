package com.ovenegash.orderworker.service;

import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.ovenegash.orderworker.client.CatalogClient;
import com.ovenegash.orderworker.client.CustomerDto;
import com.ovenegash.orderworker.client.ProductDto;
import com.ovenegash.orderworker.domain.OrderMessage;
import com.ovenegash.orderworker.domain.OrderProduct;
import com.ovenegash.orderworker.domain.ProcessedOrder;
import com.ovenegash.orderworker.exception.OrderRejectedException;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class OrderEnrichmentService {

    private final CatalogClient catalogClient;
    private final Clock clock;

    public Mono<ProcessedOrder> enrich(OrderMessage order) {
        Mono<CustomerDto> activeCustomer = catalogClient.getCustomer(order.customerId())
                .switchIfEmpty(Mono.error(() -> new OrderRejectedException(
                        "Cliente no existe: " + order.customerId())))
                .filter(CustomerDto::active)
                .switchIfEmpty(Mono.error(() -> new OrderRejectedException(
                        "Cliente inactivo: " + order.customerId())));

        Mono<Map<String, ProductDto>> productsById =
                catalogClient.getProducts(new LinkedHashSet<>(order.products()));

        return Mono.zip(activeCustomer, productsById)
                .map(customerAndProducts -> buildProcessedOrder(order, customerAndProducts.getT2()));
    }

    private ProcessedOrder buildProcessedOrder(OrderMessage order, Map<String, ProductDto> productsById) {
        List<String> missingProductIds = order.products().stream()
                .filter(productId -> !productsById.containsKey(productId))
                .distinct()
                .toList();
        if (!missingProductIds.isEmpty()) {
            throw new OrderRejectedException("Productos no existen en el catálogo: " + missingProductIds);
        }
        List<OrderProduct> orderProducts = order.products().stream()
                .map(productsById::get)
                .map(product -> new OrderProduct(product.productId(), product.name(), product.price()))
                .toList();
        return new ProcessedOrder(null, order.orderId(), order.customerId(), orderProducts, clock.instant());
    }
}
