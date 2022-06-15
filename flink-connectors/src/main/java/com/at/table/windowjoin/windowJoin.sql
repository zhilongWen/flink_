
-- https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/window-join/

create table if not exists left_table(
    row_time timestamp(3),
    num int,
    id string,
    watermark for row_time as row_time - interval '1' second
)

+------------------+-----+----+
|         row_time | num | id |
+------------------+-----+----+
| 2020-04-15 12:02 |   1 | L1 |
| 2020-04-15 12:06 |   2 | L2 |
| 2020-04-15 12:03 |   3 | L3 |
+------------------+-----+----+


create table if not exists right_table(
    row_time timestamp(3),
    num int,
    id string,
    watermark for row_time as row_time - interval '1' second
)
+------------------+-----+----+
|         row_time | num | id |
+------------------+-----+----+
| 2020-04-15 12:01 |   2 | R2 |
| 2020-04-15 12:04 |   3 | R3 |
| 2020-04-15 12:05 |   4 | R4 |
+------------------+-----+----+


-- =====================================================================================


select
    *
from
table(
    tumble(
        table left_table,
        descriptor(row_time),
        interval '5' minutes
    )
);

select
    *
from
table(
    tumble(
        table right_table,
        descriptor(row_time),
        interval '5' minutes
    )
);


select
    *
from
(
    select
        *
    from
    table(
        tumble(
            table left_table,
                descriptor(row_time),
                interval '5' minutes
            )
        )
) L
full join
(
    select
        *
    from
    table(
        tumble(
            table right_table,
            descriptor(row_time),
            interval '5' minutes
        )
    )
) R
on L.num = R.num and L.window_start = R.window_start and L.window_end = R.window_end

+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
|                row_time |         num |                             id |            window_start |              window_end |             window_time |               row_time0 |        num0 |                            id0 |           window_start0 |             window_end0 |            window_time0 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| 2020-04-15 12:02:00.000 |           1 |                             L1 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |                  <NULL> |      <NULL> |                         <NULL> |                  <NULL> |                  <NULL> |                  <NULL> |
| 2020-04-15 12:06:00.000 |           2 |                             L2 | 2020-04-15 12:05:00.000 | 2020-04-15 12:10:00.000 | 2020-04-15 12:09:59.999 |                  <NULL> |      <NULL> |                         <NULL> |                  <NULL> |                  <NULL> |                  <NULL> |
| 2020-04-15 12:03:00.000 |           3 |                             L3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 | 2020-04-15 12:04:00.000 |           3 |                             R3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
|                  <NULL> |      <NULL> |                         <NULL> |                  <NULL> |                  <NULL> |                  <NULL> | 2020-04-15 12:01:00.000 |           2 |                             R2 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
|                  <NULL> |      <NULL> |                         <NULL> |                  <NULL> |                  <NULL> |                  <NULL> | 2020-04-15 12:05:00.000 |           4 |                             R4 | 2020-04-15 12:05:00.000 | 2020-04-15 12:10:00.000 | 2020-04-15 12:09:59.999 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+



-- =========================================================================================

select
    *
from
(select * from table(tumble(table left_table,descriptor(row_time),interval '5' minutes))) L
where L.num in (
    select
        num
    from
         (select * from table(tumble(table right_table,descriptor(row_time),interval '5' minutes))) R
    where L.window_start = R.window_start and L.window_end = R.window_end
)

+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
|                row_time |         num |                             id |            window_start |              window_end |             window_time |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| 2020-04-15 12:03:00.000 |           3 |                             L3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+


select
    *
from
(select * from table(tumble(table left_table,descriptor(row_time),interval '5' minutes))) L
where exists(
    select * from (select * from table(tumble(table right_table,descriptor(row_time),interval '5' minutes))) R
    where L.num = R.num and L.window_start = R.window_start and L.window_end = R.window_end
)

+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
|                row_time |         num |                             id |            window_start |              window_end |             window_time |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+
| 2020-04-15 12:03:00.000 |           3 |                             L3 | 2020-04-15 12:00:00.000 | 2020-04-15 12:05:00.000 | 2020-04-15 12:04:59.999 |
+-------------------------+-------------+--------------------------------+-------------------------+-------------------------+-------------------------+




SELECT *
FROM (
         SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
     ) L WHERE L.num NOT IN (
    SELECT num FROM (
                        SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
                    ) R WHERE L.window_start = R.window_start AND L.window_end = R.window_end);
+------------------+-----+----+------------------+------------------+-------------------------+
|         row_time | num | id |     window_start |       window_end |            window_time  |
+------------------+-----+----+------------------+------------------+-------------------------+
| 2020-04-15 12:02 |   1 | L1 | 2020-04-15 12:00 | 2020-04-15 12:05 | 2020-04-15 12:04:59.999 |
| 2020-04-15 12:06 |   2 | L2 | 2020-04-15 12:05 | 2020-04-15 12:10 | 2020-04-15 12:09:59.999 |
+------------------+-----+----+------------------+------------------+-------------------------+



SELECT *
FROM (
         SELECT * FROM TABLE(TUMBLE(TABLE LeftTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
     ) L WHERE NOT EXISTS (
        SELECT * FROM (
                          SELECT * FROM TABLE(TUMBLE(TABLE RightTable, DESCRIPTOR(row_time), INTERVAL '5' MINUTES))
                      ) R WHERE L.num = R.num AND L.window_start = R.window_start AND L.window_end = R.window_end);
+------------------+-----+----+------------------+------------------+-------------------------+
|         row_time | num | id |     window_start |       window_end |            window_time  |
+------------------+-----+----+------------------+------------------+-------------------------+
| 2020-04-15 12:02 |   1 | L1 | 2020-04-15 12:00 | 2020-04-15 12:05 | 2020-04-15 12:04:59.999 |
| 2020-04-15 12:06 |   2 | L2 | 2020-04-15 12:05 | 2020-04-15 12:10 | 2020-04-15 12:09:59.999 |
+------------------+-----+----+------------------+------------------+-------------------------+







