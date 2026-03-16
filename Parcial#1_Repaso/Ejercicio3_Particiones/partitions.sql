/*
===========================================================
EJERCICIO COMPLETO: PARTICIONAMIENTO EN POSTGRESQL
===========================================================

CASO (realista):
Una empresa global de e-commerce registra ~600M filas/año en una tabla de órdenes.
Consultas más frecuentes:
  1) Revenue por mes/año (por fecha)
  2) Revenue por región (AMERICA, EUROPE, ASIA)
  3) Análisis por cliente individual (customer_id)

TABLA BASE (conceptual):
orders (
  order_id BIGINT,
  order_date DATE,
  customer_id BIGINT,
  region VARCHAR(20),
  product_id BIGINT,
  quantity INT,
  total_amount NUMERIC(12,2),
  discount NUMERIC(5,4)
)

PREGUNTAS A RESPONDER:
1) ¿Qué tipo de partición elegirías como principal y por qué?
2) Implementa particionamiento por RANGE (order_date) con particiones para:
     - Enero 2025
     - Febrero 2025
3) Implementa una tabla alternativa con partición por LIST (region)
4) Implementa una tabla alternativa con partición por HASH (customer_id)

RESPUESTA ESPERADA (resumen):
- Principal recomendada: RANGE por order_date
  Porque la mayoría de queries y mantenimiento son temporales:
    - partition pruning en filtros por fecha
    - fácil borrar datos antiguos: DROP PARTITION (DROP TABLE de partición)
- LIST por region: útil cuando hay pocas categorías fijas (AMERICA/EUROPE/ASIA)
- HASH por customer_id: útil para distribución uniforme cuando se consulta por cliente
  (no ayuda para rangos de fecha).
===========================================================
*/
-- ========================================================
-- (1) SOLUCIÓN PRINCIPAL: PARTICIONAMIENTO POR RANGE (FECHA)
-- =========================================================
/*
ENUNCIADO:
Crear tabla orders_range particionada por RANGE en order_date.
Crear particiones mensuales para:
  - 2025-01
  - 2025-02

RESPUESTA:
RANGE es la mejor opción principal porque:
- Queries típicas filtran por fechas (mes/año).
- Partition pruning reduce lecturas.
- Borrado histórico es simple: dropear particiones antiguas.
*/

CREATE TABLE orders_range (
    order_id      BIGINT NOT NULL,
    order_date    DATE   NOT NULL,
    customer_id   BIGINT NOT NULL,
    region        VARCHAR(20) NOT NULL,
    product_id    BIGINT NOT NULL,
    quantity      INT NOT NULL,
    total_amount  NUMERIC(12,2) NOT NULL,
    discount      NUMERIC(5,4)  NOT NULL,
    PRIMARY KEY (order_id, order_date)  -- incluye columna partición
) PARTITION BY RANGE (order_date);

-- Partición: Enero 2025  [2025-01-01, 2025-02-01)
CREATE TABLE orders_range_2025_01
PARTITION OF orders_range
FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

-- =========================================================
-- (2) ALTERNATIVA: PARTICIONAMIENTO POR LIST (REGION)
-- =========================================================
/*
ENUNCIADO:
Crear tabla orders_list particionada por LIST(region).
Crear particiones para AMERICA, EUROPE, ASIA.

RESPUESTA:
LIST conviene cuando:
- hay un conjunto pequeño/fijo de valores (regiones)
- queries filtran continuamente por región
*/

CREATE TABLE orders_list (
    order_id      BIGINT NOT NULL,
    order_date    DATE   NOT NULL,
    customer_id   BIGINT NOT NULL,
    region        VARCHAR(20) NOT NULL,
    total_amount  NUMERIC(12,2) NOT NULL,
    discount      NUMERIC(5,4)  NOT NULL,
    PRIMARY KEY (order_id, region)   -- aquí la partición es region
) PARTITION BY LIST (region);

CREATE TABLE orders_list_america
PARTITION OF orders_list
FOR VALUES IN ('AMERICA');

CREATE TABLE orders_list_europe
PARTITION OF orders_list
FOR VALUES IN ('EUROPE');

-- =========================================================
-- (3) ALTERNATIVA: PARTICIONAMIENTO POR HASH (CUSTOMER_ID)
-- =========================================================
/*
ENUNCIADO:
Crear tabla orders_hash particionada por HASH(customer_id).
Crear 4 particiones (modulus 4).

RESPUESTA:
HASH conviene cuando:
- se consulta mucho por customer_id (acceso puntual)
- se quiere distribución uniforme de filas por partición
NO es ideal para rangos de fecha.
*/

CREATE TABLE orders_hash (
    order_id      BIGINT NOT NULL,
    order_date    DATE   NOT NULL,
    customer_id   BIGINT NOT NULL,
    total_amount  NUMERIC(12,2) NOT NULL,
    discount      NUMERIC(5,4)  NOT NULL,
    PRIMARY KEY (order_id, customer_id) -- partición es customer_id
) PARTITION BY HASH (customer_id);

CREATE TABLE orders_hash_p0
PARTITION OF orders_hash
FOR VALUES WITH (MODULUS 4, REMAINDER 0);

CREATE TABLE orders_hash_p1
PARTITION OF orders_hash
FOR VALUES WITH (MODULUS 4, REMAINDER 1);

CREATE TABLE orders_hash_p2
PARTITION OF orders_hash
FOR VALUES WITH (MODULUS 4, REMAINDER 2);

CREATE TABLE orders_hash_p3
PARTITION OF orders_hash
FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- =========================================================
-- FIN DEL SCRIPT
-- =========================================================