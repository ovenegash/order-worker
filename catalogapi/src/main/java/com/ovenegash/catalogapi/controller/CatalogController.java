package com.ovenegash.catalogapi.controller;

import com.ovenegash.catalogapi.model.Customer;
import com.ovenegash.catalogapi.model.Product;
import com.ovenegash.catalogapi.repository.CatalogRepository;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class CatalogController {

    private final CatalogRepository repository;

    @GetMapping("/products/{id}")
    public Mono<ResponseEntity<Product>> getProduct(@PathVariable String id) {
        return repository.findProduct(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/products")
    public Flux<Product> getProducts(@RequestParam List<String> ids) {
        return repository.findProducts(ids);
    }

    @GetMapping("/customers/{id}")
    public Mono<ResponseEntity<Customer>> getCustomer(@PathVariable String id) {
        return repository.findCustomer(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }
}
