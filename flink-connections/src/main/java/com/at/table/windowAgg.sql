
CREATE TABLE IF NOT EXISTS bid_tbl(
    bidtime timestamp(3),
    price decimal(10,2),
    item string,
    supplier_id string,
    watermark for bidtime as bidtime - interval '1' second
)

SELECT * FROM Bid;

+------------------+-------+------+-------------+
|          bidtime | price | item | supplier_id |
+------------------+-------+------+-------------+
| 2020-04-15 08:05 | 4.00  | C    | supplier1   |
| 2020-04-15 08:07 | 2.00  | A    | supplier1   |
| 2020-04-15 08:09 | 5.00  | D    | supplier2   |
| 2020-04-15 08:11 | 3.00  | B    | supplier2   |
| 2020-04-15 08:13 | 1.00  | E    | supplier1   |
| 2020-04-15 08:17 | 6.00  | F    | supplier2   |
+------------------+-------+------+-------------+


-- =========================================== tumbling window aggregation

SELECT
    window_start,
    window_end,
    sum(price)
FROM
TABLE
(
    TUMBLE(
        TABLE bid_tbl,
        DESCRIPTOR(bidtime),
        INTERVAL  '10' MINUTES
    )
)
GROUP BY window_start,window_end;




-- =========================================== hopping window aggregation

SELECT
    window_start,
    window_end,
    sum(price)
FROM
TABLE
(
    HOP(
        TABLE bid_tbl,
        DESCRIPTOR(bidtime),
        INTERVAL '2' MINUTES,
        INTERVAL '10' MINUTES
    )
)
GROUP BY window_start,window_end;







