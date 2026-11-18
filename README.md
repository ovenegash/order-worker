# Order Worker

Prueba técnica: worker en Java 21 + Spring Boot WebFlux que consume pedidos desde Kafka, los enriquece con una API de catálogo, valida y guarda en MongoDB. Usa Redis para reintentos de pedidos fallidos y para el lock distribuido.

La API de catálogo la hice en Java (Spring WebFlux) en vez de Go, el enunciado lo permitía.

## Estructura

- `catalogapi/`: API de productos y clientes con datos en memoria (puerto 8081). Tiene un `ChaosFilter` para simular errores 503 y latencia (`chaos.failure-rate`, `chaos.delay-ms`), lo usé para probar los reintentos.
- `orderworker/`: el worker.
- `docker-compose.yml`: Kafka, Kafka UI, MongoDB y Redis. Con el perfil `apps` levanta también las dos aplicaciones.

## Cómo funciona

1. `OrderListener` lee del tópico `orders`. Formato del mensaje:
   ```json
   {"orderId":"order-123","customerId":"customer-456","products":["product-789","product-101"]}
   ```
2. Se valida el mensaje. Si el JSON está malo o faltan campos se rechaza sin reintentar.
3. Se toma un lock en Redis por pedido (`lock:order:{orderId}`, `SET NX PX`). Si otra instancia ya lo tiene, se omite.
4. Si el pedido ya está en Mongo no se vuelve a guardar (además hay índice único en `orderId`).
5. Se consultan cliente y productos al catálogo en paralelo. Los productos van en una sola llamada.
6. Se valida que el cliente exista y esté activo y que todos los productos existan.
7. Se guarda en la colección `orders`.

### Reintentos

- Cada llamada al catálogo tiene timeout y reintento exponencial con jitter (solo 5xx, 429, timeout o error de conexión; un 404 no se reintenta).
- Si igual falla, el pedido queda en Redis (`orders:failed:{orderId}`) con el payload y el contador de intentos, y se agenda en `orders:retry-queue`. Un scheduler lo reintenta con backoff exponencial y al llegar al máximo lo pasa a `orders:dead-letter`.
- Los rechazos por reglas de negocio van directo a `orders:dead-letter` con el motivo.

Todo es configurable en `application.yaml` bajo `worker.*` (intentos, tiempos de espera, TTL del lock, TTL de la caché).

### Escalabilidad

- Todo el flujo es reactivo (WebClient, Mongo y Redis reactivos).
- El tópico tiene 3 particiones y el listener concurrencia 3. Se pueden levantar más instancias con el mismo group-id; el lock y el índice único evitan procesar dos veces.
- Caché local con TTL para productos y clientes.

## Ejecutar

Requisitos: Java 21 y Docker.

```bash
docker compose up -d
```

```bash
cd catalogapi && ./mvnw spring-boot:run
```

```bash
cd orderworker && ./mvnw spring-boot:run
```

O todo con Docker:

```bash
docker compose --profile apps up -d --build
```

Para mandar pedidos se puede usar Kafka UI (http://localhost:8090, tópico `orders` → Produce Message) o la consola:

```bash
docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders --property parse.key=true --property key.separator='|'
```

```
order-123|{"orderId":"order-123","customerId":"customer-456","products":["product-789","product-101"]}
order-200|{"orderId":"order-200","customerId":"customer-999","products":["product-789"]}
order-300|{"orderId":"order-300","customerId":"customer-456","products":["product-000"]}
```

Datos de prueba: `customer-456` activo, `customer-999` inactivo, productos `product-789`, `product-101` y `product-202`.

Revisar resultados:

```bash
docker exec mongodb mongosh --quiet orders_db --eval "db.orders.find()"
```

```bash
docker exec redis redis-cli SMEMBERS orders:dead-letter
```

## Tests

```bash
cd orderworker && ./mvnw test
```

Hay tests unitarios del cliente HTTP (reintentos, 404, caché), del enriquecimiento (cliente inactivo, inexistente, producto faltante) y del procesador (lock, duplicados, rechazo, reintento, dead-letter, JSON inválido). `OrderworkerApplicationTests` necesita la infraestructura arriba y se corre con `INTEGRATION=true`.

## Notas

- Usé Spring Kafka con listener que retorna `Mono` en vez de Reactor Kafka, que está discontinuado.
- El enunciado habla de "clientes bloqueados" pero describe un lock por pedido, así que lo implementé por pedido.
- La caché es local por instancia; con TTL corto me pareció suficiente para este caso.
