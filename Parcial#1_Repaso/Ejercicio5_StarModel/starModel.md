### 📝 Enunciado tipo examen (corto, pero te hace razonar)

Una app de delivery (comida) quiere una capa **GOLD** en esquema estrella para BI.

Tablas RAW (mínimas):

* `raw.orders` (`order_id`, `order_ts`, `customer_id`, `restaurant_id`, `courier_id`, `city_id`, `status`)
* `raw.order_items` (`order_id`, `line_number`, `product_id`, `qty`, `unit_price`)
* `raw.payments` (`order_id`, `payment_method`, `subtotal`, `delivery_fee`, `tip_amount`, `discount_amount`)
* `raw.promotions` (`order_id`, `promo_code`) *(puede venir null)*
* `raw.customers` (`customer_id`, `segment`)
* `raw.restaurants` (`restaurant_id`, `restaurant_name`, `cuisine`)
* `raw.products` (`product_id`, `product_name`, `category`)
* `raw.cities` (`city_id`, `city_name`, `region`, `country`)

**Pide:**

1. Diseña un **modelo estrella** que responda:

   * Revenue neto por `year_month`, `region` y `cuisine`
   * Top 10 promo_codes por revenue neto (y su % de órdenes)
   * Tiempo promedio de “ciclo” por courier (solo órdenes delivered) *(asume existe `order_ts` y `delivered_ts` en orders para este cálculo)*
2. Define el **grain** de la fact.
3. Escribe solo los SQL de **creación** (DDL) para la estructura GOLD.
4. Explica en 4–5 líneas por qué el modelo es correcto.

---

## ✅ Respuesta esperada (versión examen, al punto)

### 1) Grain

**1 fila por línea de ítem**: `(order_id, line_number)`
Porque el mix de productos y unidades exige nivel ítem; pagos/promo se “repiten” por línea para poder sumar sin volver a RAW.

---

### 2) Esquema estrella (1 fact + dims)

**Fact:** `gold.fact_order_items`
**Dims mínimas:**

* `gold.dim_date`
* `gold.dim_city` (incluye region/country)
* `gold.dim_customer` (segment)
* `gold.dim_restaurant` (cuisine)
* `gold.dim_product` (category)
* `gold.dim_payment_method`
* `gold.dim_promotion` (promo_code, incluye “NO_PROMO”)

---

## 3) SQL DDL para crear estructura GOLD (solo creación)

### 01_create_schema.sql

```sql
create schema if not exists gold;
```

### 02_create_gold_tables.sql

```sql
-- DIMENSIONS
create table if not exists gold.dim_date (
  date_key int primary key,          -- yyyymmdd
  date date not null,
  year int not null,
  month int not null,
  year_month varchar(7) not null     -- YYYY-MM
);

create table if not exists gold.dim_city (
  city_key int generated always as identity primary key,
  city_id bigint unique not null,
  city_name varchar,
  region varchar,
  country varchar
);

create table if not exists gold.dim_customer (
  customer_key int generated always as identity primary key,
  customer_id bigint unique not null,
  segment varchar
);

create table if not exists gold.dim_restaurant (
  restaurant_key int generated always as identity primary key,
  restaurant_id bigint unique not null,
  restaurant_name varchar,
  cuisine varchar
);

create table if not exists gold.dim_product (
  product_key int generated always as identity primary key,
  product_id bigint unique not null,
  product_name varchar,
  category varchar
);

create table if not exists gold.dim_payment_method (
  payment_method_key int generated always as identity primary key,
  payment_method varchar unique not null
);

create table if not exists gold.dim_promotion (
  promotion_key int generated always as identity primary key,
  promo_code varchar unique not null
);

-- FACT (1 row per order line)
create table if not exists gold.fact_order_items (
  order_id bigint not null,
  line_number int not null,

  order_date_key int not null,            -- FK to dim_date
  delivered_date_key int,                 -- FK to dim_date (solo si delivered)
  city_key int not null,
  customer_key int not null,
  restaurant_key int not null,
  courier_id bigint,                      -- degenerate dimension simple
  product_key int not null,
  payment_method_key int not null,
  promotion_key int not null,

  status varchar,

  qty int not null,
  unit_price numeric(12,2) not null,

  -- Métricas a nivel línea (para poder agregar sin volver a RAW)
  gross_item_amount numeric(14,2) not null,
  -- Reparto proporcional de descuentos/fees/tips por línea:
  allocated_discount_amount numeric(14,2) not null,
  allocated_delivery_fee numeric(14,2) not null,
  allocated_tip_amount numeric(14,2) not null,
  net_revenue numeric(14,2) not null,

  dispatch_minutes int,                   -- delivered_ts - order_ts (si aplica)

  primary key (order_id, line_number)
);

-- Índices típicos para BI
create index if not exists idx_fact_date on gold.fact_order_items(order_date_key);
create index if not exists idx_fact_rest on gold.fact_order_items(restaurant_key);
create index if not exists idx_fact_city on gold.fact_order_items(city_key);
```

---

## 4) Por qué este modelo es correcto (4–5 líneas)

* La fact tiene **grain de ítem**, lo que permite analizar **mix de productos** y **unidades** sin ambigüedad.
* Las dimensiones concentran atributos descriptivos (fecha, ciudad/región, cliente, restaurante/cocina, producto, promo, pago), habilitando `GROUP BY` para BI.
* Métricas están a **nivel línea** (gross y net con asignación), así se puede sumar por cualquier dimensión sin ir a RAW.
* `order_id` y `courier_id` pueden ser **degenerate dimensions** (valen como identificadores en la fact sin tabla aparte).
* El modelo soporta las 3 preguntas solo con `fact ⨝ dims`.

