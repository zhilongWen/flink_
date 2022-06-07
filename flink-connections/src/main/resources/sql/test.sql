
CREATE TABLE rule_tbl(
    user_id int,
    name string,
    age int,
    sex int,
    address string
) comment '测试flink读写hive数据'
    partitioned by (`dt` STRING,`hm` STRING)
    stored as orc
    location '/user/hive/warehouse/rule_tbl'
    tblproperties("orc.compress" = "snappy")



insert into rule_tbl values(1016727,'Mary',10,1,'中国北京市')







