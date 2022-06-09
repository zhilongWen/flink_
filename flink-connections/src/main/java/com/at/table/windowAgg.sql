

-- https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/window-agg/

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

+-------------------------+-------------------------+--------------------------------+
|            window_start |              window_end |                         EXPR$2 |
+-------------------------+-------------------------+--------------------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                           11.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                           10.0 |
+-------------------------+-------------------------+--------------------------------+


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

+-------------------------+-------------------------+--------------------------------+
|            window_start |              window_end |                         EXPR$2 |
+-------------------------+-------------------------+--------------------------------+
| 2020-04-15 08:04:00.000 | 2020-04-15 08:14:00.000 |                           15.0 |
| 2020-04-15 08:02:00.000 | 2020-04-15 08:12:00.000 |                           14.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                           11.0 |
| 2020-04-15 07:58:00.000 | 2020-04-15 08:08:00.000 |                            6.0 |
| 2020-04-15 07:56:00.000 | 2020-04-15 08:06:00.000 |                            4.0 |
| 2020-04-15 08:06:00.000 | 2020-04-15 08:16:00.000 |                           11.0 |
| 2020-04-15 08:08:00.000 | 2020-04-15 08:18:00.000 |                           15.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                           10.0 |
| 2020-04-15 08:12:00.000 | 2020-04-15 08:22:00.000 |                            7.0 |
| 2020-04-15 08:16:00.000 | 2020-04-15 08:26:00.000 |                            6.0 |
| 2020-04-15 08:14:00.000 | 2020-04-15 08:24:00.000 |                            6.0 |
+-------------------------+-------------------------+--------------------------------+


-- =========================================== GROUPING SETS

SELECT
    window_start,
    window_end,
    supplier_id,
    sum(price) as price
FROM
    TABLE
        (
            TUMBLE(
                TABLE bid_tbl,
                    DESCRIPTOR(bidtime),
                    INTERVAL  '10' MINUTES
                )
        )
GROUP BY window_start,window_end,GROUPING SETS((supplier_id),());

+-------------------------+-------------------------+--------------------------------+--------------------------------+
|            window_start |              window_end |                    supplier_id |                          price |
+-------------------------+-------------------------+--------------------------------+--------------------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier1 |                            6.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                           11.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier2 |                            5.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                      supplier2 |                            9.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                           10.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                      supplier1 |                            1.0 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+

2020-04-15 08:05  4.00   C     supplier1        2020-04-15 08:05  4.00   C     supplier1   2020-04-15 08:00:00.000  2020-04-15 08:10:00.000           2020-04-15 08:00:00.000  2020-04-15 08:10:00.000  supplier1  6.00
2020-04-15 08:07  2.00   A     supplier1        2020-04-15 08:07  2.00   A     supplier1   2020-04-15 08:00:00.000  2020-04-15 08:10:00.000     =>    2020-04-15 08:00:00.000  2020-04-15 08:10:00.000  supplier2  5.00
2020-04-15 08:09  5.00   D     supplier2        2020-04-15 08:09  5.00   D     supplier2   2020-04-15 08:00:00.000  2020-04-15 08:10:00.000           2020-04-15 08:00:00.000  2020-04-15 08:10:00.000             11.00
                                            =>
2020-04-15 08:11  3.00   B     supplier2        2020-04-15 08:11  3.00   B     supplier2   2020-04-15 08:10:00.000  2020-04-15 08:20:00.000           2020-04-15 08:10:00.000  2020-04-15 08:20:00.000  supplier2 9.00
2020-04-15 08:13  1.00   E     supplier1        2020-04-15 08:13  1.00   E     supplier1   2020-04-15 08:10:00.000  2020-04-15 08:20:00.000     =>    2020-04-15 08:10:00.000  2020-04-15 08:20:00.000  supplier1 1.00
2020-04-15 08:17  6.00   F     supplier2        2020-04-15 08:17  6.00   F     supplier2   2020-04-15 08:10:00.000  2020-04-15 08:20:00.000           2020-04-15 08:10:00.000  2020-04-15 08:20:00.000            10.00

SELECT
    window_start,
    window_end,
    item,
    supplier_id,
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
GROUP BY window_start,window_end,
    GROUPING SETS(
    (supplier_id, item),
    (supplier_id      ),
    (             item),
    (                 )
    );

+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
|            window_start |              window_end |                           item |                    supplier_id |                         EXPR$4 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              C |                      supplier1 |                            4.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                      supplier1 |                            6.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              C |                         <NULL> |                            4.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                         <NULL> |                           11.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              A |                      supplier1 |                            2.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              A |                         <NULL> |                            2.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              D |                      supplier2 |                            5.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                      supplier2 |                            5.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              D |                         <NULL> |                            5.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              B |                      supplier2 |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                      supplier2 |                            9.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              B |                         <NULL> |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                         <NULL> |                           10.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              E |                      supplier1 |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                      supplier1 |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              E |                         <NULL> |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              F |                      supplier2 |                            6.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              F |                         <NULL> |                            6.0 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+

-- =========================================== CUBE

SELECT
    window_start,
    window_end,
    item,
    supplier_id,
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
GROUP BY window_start,window_end, CUBE (supplier_id, item);

+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
|            window_start |              window_end |                           item |                    supplier_id |                         EXPR$4 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              C |                      supplier1 |                            4.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                      supplier1 |                            6.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              C |                         <NULL> |                            4.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                         <NULL> |                           11.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              A |                      supplier1 |                            2.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              A |                         <NULL> |                            2.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              D |                      supplier2 |                            5.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                         <NULL> |                      supplier2 |                            5.0 |
| 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                              D |                         <NULL> |                            5.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              B |                      supplier2 |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                      supplier2 |                            9.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              B |                         <NULL> |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                         <NULL> |                           10.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              E |                      supplier1 |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                         <NULL> |                      supplier1 |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              E |                         <NULL> |                            1.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              F |                      supplier2 |                            6.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:20:00.000 |                              F |                         <NULL> |                            6.0 |
+-------------------------+-------------------------+--------------------------------+--------------------------------+--------------------------------+



-- tumbling 5 minutes for each supplier_id
CREATE VIEW window1 AS
-- Note: The window start and window end fields of inner Window TVF are optional in the select clause. However, if they appear in the clause, they need to be aliased to prevent name conflicting with the window start and window end of the outer Window TVF.
SELECT
    window_start as window_5mintumble_start,
    window_end as window_5mintumble_end,
    window_time as rowtime,
    SUM(price) as partial_price
FROM
TABLE(
    TUMBLE(
        TABLE bid_tbl,
        DESCRIPTOR(bidtime),
        INTERVAL '5' MINUTE
    )
)
GROUP BY supplier_id,window_start,window_end,window_time

+-------------------------+-------------------------+-------------------------+--------------------------------+
| window_5mintumble_start |   window_5mintumble_end |                 rowtime |                  partial_price |
+-------------------------+-------------------------+-------------------------+--------------------------------+
| 2020-04-15 08:05:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |                            6.0 |
| 2020-04-15 08:05:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:59.999 |                            5.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |                            3.0 |
| 2020-04-15 08:10:00.000 | 2020-04-15 08:15:00.000 | 2020-04-15 08:14:59.999 |                            1.0 |
| 2020-04-15 08:15:00.000 | 2020-04-15 08:20:00.000 | 2020-04-15 08:19:59.999 |                            6.0 |
+-------------------------+-------------------------+-------------------------+--------------------------------+



-- tumbling 5 minutes for each supplier_id
CREATE VIEW window1 AS
-- Note: The window start and window end fields of inner Window TVF are optional in the select clause. However, if they appear in the clause, they need to be aliased to prevent name conflicting with the window start and window end of the outer Window TVF.
SELECT window_start as window_5mintumble_start, window_end as window_5mintumble_end, window_time as rowtime, SUM(price) as partial_price
FROM TABLE(
        TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '5' MINUTES))
GROUP BY supplier_id, window_start, window_end, window_time;

-- tumbling 10 minutes on the first window
SELECT window_start, window_end, SUM(partial_price) as total_price
FROM TABLE(
        TUMBLE(TABLE window1, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))
GROUP BY window_start, window_end;



