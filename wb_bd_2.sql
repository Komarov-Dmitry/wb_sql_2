--ЧАСТЬ 1

-- ЗАДАНИЕ 1:
--Найти клиента с самым долгим временем ожидания между заказом и доставкой. Для этой задачи у вас есть таблицы "Customers", "Orders"

WITH wait_time AS (
    -- Я создаю временную таблицу под названием `wait_time`, которая содержит данные о клиентах, заказах и времени доставки.
    SELECT
        c.customer_id, -- Идентификатор клиента
        c.name,        -- Имя клиента
        o.order_id,    -- Идентификатор заказа
        o.order_date,  -- Дата заказа
        o.shipment_date, -- Дата отправки
        CAST(o.shipment_date AS TIMESTAMP) - CAST(o.order_date AS TIMESTAMP) AS delivery_time
        -- Я вычисляю время доставки (разница между датой отправки и датой заказа), используя приведение типов к TIMESTAMP.
    FROM
        customers_new c -- Таблица с информацией о клиентах
    JOIN
        orders_new o     -- Таблица с информацией о заказах
    ON
        c.customer_id = o.customer_id
        -- Я соединяю таблицы `customers_new` и `orders_new` по идентификатору клиента.
)

-- Теперь я выбираю данные из временной таблицы `wait_time`.
SELECT
    customer_id, -- Идентификатор клиента
    name,        -- Имя клиента
    delivery_time -- Время доставки
FROM
    wait_time
WHERE
    delivery_time = (SELECT MAX(delivery_time) FROM wait_time)
    -- Я выбираю только те записи, у которых самое длинное время доставки



--ЗАДАНИЕ 2:
--Найти клиентов, сделавших наибольшее количество заказов, и для каждого из них найти среднее время между заказом и доставкой, а также общую сумму всех их заказов. Вывести клиентов в порядке убывания общей суммы заказов.

WITH customer_orders AS (
    -- Я создаю временную таблицу `customer_orders`, в которой подсчитываю заказы, среднее время доставки и общую сумму заказов для каждого клиента.
    SELECT 
        c.customer_id, -- Идентификатор клиента
        c.name,        -- Имя клиента
        COUNT(o.order_id) AS order_count,
        -- Я считаю количество заказов (order_count) для каждого клиента.
        AVG(o.shipment_date::timestamp - o.order_date::timestamp) AS avg_delivery_time,
        -- Я вычисляю среднее время доставки (avg_delivery_time), преобразуя даты в TIMESTAMP и вычисляя разницу.
        SUM(o.order_ammount) AS total_order_amount
        -- Я суммирую сумму всех заказов (total_order_amount) клиента.
    FROM 
        customers_new c -- Таблица с информацией о клиентах
    JOIN 
        orders_new o ON c.customer_id = o.customer_id
        -- Я связываю таблицы `customers_new` и `orders_new` по идентификатору клиента.
    GROUP BY 
        c.customer_id, c.name
        -- Я группирую данные по клиенту (по customer_id и имени), чтобы агрегировать информацию о заказах.
)

-- Теперь я выбираю данные из временной таблицы `customer_orders`.
SELECT 
    customer_id, -- Идентификатор клиента
    name,        -- Имя клиента
    order_count, -- Общее количество заказов
    avg_delivery_time, -- Среднее время доставки
    total_order_amount -- Общая сумма заказов
FROM 
    customer_orders
WHERE 
    order_count = (SELECT MAX(order_count) FROM customer_orders)
    -- Я выбираю только тех клиентов, у которых максимальное количество заказов (order_count).
ORDER BY total_order_amount desc -- Я сортирую в порядке убывания общей суммы заказов.



--ЗАДАНИЕ 3:
--Найти клиентов, у которых были заказы, доставленные с задержкой более чем на 5 дней, и клиентов, у которых были заказы, которые были отменены. Для каждого клиента вывести имя, количество доставок с задержкой, количество отмененных заказов и их общую сумму. Результат отсортировать по общей сумме заказов в убывающем порядке.


SELECT 
    c.name AS customer_name,
    -- Я выбираю имя клиента и присваиваю этому столбцу имя `customer_name`.

    COUNT(CASE WHEN o.order_status = 'Approved' AND (o.shipment_date::date - o.order_date::date) > 5 THEN 1 END) AS delayed_deliveries,
    -- Я считаю количество заказов, которые были "Approved" и доставлены с задержкой (более 5 дней).
    -- Использую `CASE` для фильтрации таких заказов и считаю их с помощью `COUNT`.

    COUNT(CASE WHEN o.order_status = 'Cancel' THEN 1 END) AS cancelled_orders,
    -- Я считаю количество заказов, которые были отменены (статус "Cancel").

    SUM(o.order_ammount) AS total_order_amount
    -- Я вычисляю общую сумму всех заказов клиента.
FROM 
    customers_new c
    -- Я использую таблицу с информацией о клиентах.

INNER JOIN 
    orders_new o ON c.customer_id = o.customer_id
    -- Я соединяю таблицы клиентов и заказов по идентификатору клиента (`customer_id`).

GROUP BY 
    c.name
    -- Я группирую данные по имени клиента, чтобы агрегировать информацию по каждому из них.

HAVING 
    COUNT(CASE WHEN o.order_status = 'Approved' AND (o.shipment_date::date - o.order_date::date) > 5 THEN 1 END) > 0
    OR COUNT(CASE WHEN o.order_status = 'Cancel' THEN 1 END) > 0
    -- Я фильтрую клиентов, у которых есть хотя бы один задержанный заказ (более 5 дней) 
    -- или хотя бы один отменённый заказ.

ORDER BY 
    total_order_amount DESC;
    -- Я сортирую результат по общей сумме заказов 




--ЧАСТЬ 2

--Задание 1:

WITH category_sales AS (
    -- Я создаю временную таблицу `category_sales`, которая хранит суммарные продажи по каждой категории товаров.
    SELECT 
        p.product_category, 
        SUM(o.order_ammount) AS total_sales
        -- Для каждой категории товаров я считаю общие продажи (сумму `order_ammount`).
    FROM 
        orders o
    JOIN 
        products p ON o.product_id = p.product_id
        -- Я связываю таблицы заказов и товаров по `product_id`, чтобы получить информацию о товарах в каждом заказе.
    GROUP BY 
        p.product_category
        -- Я группирую данные по категориям товаров, чтобы подсчитать суммарные продажи по каждой категории.
),
product_sales AS (
    -- Я создаю временную таблицу `product_sales`, которая хранит суммарные продажи по каждому товару.
    SELECT 
        p.product_category,
        p.product_name,
        SUM(o.order_ammount) AS total_sales
        -- Для каждого товара я считаю общие продажи (сумму `order_ammount`), группируя их по категории и названию товара.
    FROM 
        orders o
    JOIN 
        products p ON o.product_id = p.product_id
        -- Я снова связываю таблицы заказов и товаров по `product_id`.
    GROUP BY 
        p.product_category, p.product_name
        -- Я группирую данные по категориям товаров и названиям товаров, чтобы подсчитать суммарные продажи по каждому товару.
),
max_product_sales AS (
    -- Я создаю временную таблицу `max_product_sales`, чтобы найти товар с максимальными продажами в каждой категории.
    SELECT 
        product_category,
        MAX(total_sales) AS max_sales
        -- Для каждой категории товаров я нахожу максимальное значение продаж среди всех товаров в этой категории.
    FROM 
        product_sales
    GROUP BY 
        product_category
        -- Я группирую данные по категориям товаров, чтобы найти товар с наибольшими продажами в каждой категории.
)
SELECT 
    cs.product_category,
    cs.total_sales AS category_total_sales,
    ps.product_name AS product_with_max_sales,
    ps.total_sales AS max_sales_for_product,
    ROUND((ps.total_sales::NUMERIC / cs.total_sales::NUMERIC) * 100, 2) AS sales_percentage
    -- Я выбираю: 
    -- 1) название категории товаров (`product_category`),
    -- 2) суммарные продажи по категории (`category_total_sales`),
    -- 3) товар с максимальными продажами в категории (`product_with_max_sales`),
    -- 4) максимальные продажи для этого товара (`max_sales_for_product`),
    -- 5) процент от продаж категории, который составляют продажи товара с максимальными продажами (`sales_percentage`).
FROM 
    category_sales cs
JOIN 
    product_sales ps ON cs.product_category = ps.product_category
    -- Я соединяю таблицу с суммарными продажами по категориям (`category_sales`) с таблицей с суммарными продажами по товарам (`product_sales`),
    -- чтобы сопоставить категории товаров с их товарами.
JOIN 
    max_product_sales mps ON ps.product_category = mps.product_category 
    AND ps.total_sales = mps.max_sales
    -- Я также соединяю таблицу с максимальными продажами по товарам (`max_product_sales`) с таблицей товаров,
    -- чтобы выбрать только те товары, которые имеют максимальные продажи в своей категории.
ORDER BY 
    cs.total_sales DESC;
    -- Я сортирую результат по общим продажам категории товаров от большего к меньшему, чтобы сначала вывести категории с наибольшими продажами

    
