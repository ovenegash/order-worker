Prueba tecnica: 🧪 Prueba Técnica: Worker Java y Go para Procesamiento de Pedidos con Enriquecimiento de Datos y Resiliencia\*\*

### 📝 Descripción General

Desarrollar un _Worker_ en **Java** para procesar pedidos de forma eficiente y confiable. Este Worker:

- Consume mensajes de un **tópico de Kafka** con información básica del pedido.
- Enriquecerá los datos consultando **APIs externas desarrolladas en Go**.
- Finalmente, almacenará los datos procesados en **MongoDB**.

---

### ✅ Requerimientos Detallados

#### 🔹 Consumo de Mensajes de Kafka

- Suscribirse a un tópico de Kafka.
- Cada mensaje contiene:
- ID de pedido
- ID del cliente
- Lista de productos

#### 🔹 Enriquecimiento de Datos

- Llamar una **API en Go** para:
- Obtener detalles de los productos (nombre, descripción, precio, etc.).
- Obtener detalles del cliente.

#### 🔹 Validación de Datos

- Validar que:
- Los productos existan en el catálogo.
- El cliente exista y esté activo.

#### 🔹 Almacenamiento en MongoDB

- Guardar los pedidos procesados con esta estructura:

```json
{
"_id": ObjectId(),
"orderId": "order-123",
"customerId": "customer-456",
"products": [
{
"productId": "product-789",
"name": "Laptop",
"price": 999
}
]
}
```

#### 🔹 Manejo de Errores y Reintentos

- Implementar **reintentos exponenciales** para llamadas a APIs.
- Usar **Redis** para almacenar mensajes fallidos + contador de intentos.
- Configurar máximo de reintentos y tiempo de espera entre ellos.

#### 🔹 Gestión de Clientes Bloqueados

- Usar **lock distribuido (Redis)** para evitar que múltiples instancias procesen el mismo pedido al mismo tiempo.

---

### 🧰 Tecnologías y Herramientas Sugeridas

- **Java 21** (obligatorio)
- **Spring Boot**, **Java Webflux** (obligatorio)
- **Go**
- **Kafka**
- **MongoDB**
- **Redis**
- **GitHub / GitLab** para versionamiento

---

### 💡 Consideraciones Adicionales

- **Diseño:** Modular y estructurado para mantenimiento.
- **Pruebas:** Incluir pruebas unitarias.
- **Rendimiento:** Optimizar con caching, índices y buenas prácticas.
- **Escalabilidad:** Pensar en volumen creciente de pedidos.

entregar repositorio github o gitlab

si hay no manejas go lo puedes cambiar por java, o nestjs
