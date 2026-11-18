package com.ovenegash.catalogapi.repository;

import com.ovenegash.catalogapi.model.Customer;
import com.ovenegash.catalogapi.model.Product;
import org.springframework.stereotype.Repository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Repository
public class CatalogRepository {

    private final Map<String, Product> products = Map.of(
            "product-789", new Product("product-789", "Laptop", "Laptop 15 pulgadas", new BigDecimal("999")),
            "product-101", new Product("product-101", "Mouse", "Mouse inalámbrico", new BigDecimal("25")),
            "product-202", new Product("product-202", "Teclado", "Teclado mecánico", new BigDecimal("80")));

    private final Map<String, Customer> customers = Map.of(
            "customer-456", new Customer("customer-456", "Juan Pérez", "juan@mail.com", true),
            "customer-999", new Customer("customer-999", "Ana Soto", "ana@mail.com", false));

    public Mono<Product> findProduct(String id) {
        return Mono.justOrEmpty(products.get(id));
    }

    public Flux<Product> findProducts(List<String> ids) {
        return Flux.fromIterable(ids).mapNotNull(products::get);
    }

    public Mono<Customer> findCustomer(String id) {
        return Mono.justOrEmpty(customers.get(id));
    }
}
