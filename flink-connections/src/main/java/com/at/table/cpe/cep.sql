
-- https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/match_recognize/

-- https://www.alibabacloud.com/help/zh/realtime-compute-for-apache-flink/latest/cep-statements


create table if not exists cep_table(
    symbol string,
    row_time timestamp(3),
    price int,
    tax int
)


-- =================================================================

match_recognize(
    partition by symbol
    order by row_time

    measures
        start_row.row_time as start_tstamp,
        last(price_down.row_time) as bottom_tstamp,
        last(price_up.row_time) as end_tstamp
    one row per match
    after match skip to last price_up
    pattern (start_row,price_down + price_up)
    define
        price_down as


)






-- https://www.alibabacloud.com/help/zh/realtime-compute-for-apache-flink/latest/cep-statements

+-------------------------+--------------------------------+--------------------------------+--------------------------------+
|                      ts |                        card_id |                       location |                        actionT |
+-------------------------+--------------------------------+--------------------------------+--------------------------------+
| 2018-04-13 12:00:00.000 |                              1 |                        Beijing |                    Consumption |
| 2018-04-13 12:05:00.000 |                              1 |                       Shanghai |                    Consumption |
| 2018-04-13 12:10:00.000 |                              1 |                       Shenzhen |                    Consumption |
| 2018-04-13 12:20:00.000 |                              1 |                        Beijing |                    Consumption |
+-------------------------+--------------------------------+--------------------------------+--------------------------------+


match_recognize (
    partition by `card_id`
    order by `ts`

    measures



)







































































