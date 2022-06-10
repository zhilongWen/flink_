-- https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/functions/systemfunctions/

-- https://www.alibabacloud.com/help/zh/realtime-compute-for-apache-flink/latest/char-length

-- set execution.result-mode=changelog;
-- set execution.result-mode=tableau;

-- ========================================================== string

select char_length('wer') var1
+-------------+
|        var1 |
+-------------+
|           3 |
+-------------+

select chr(97)
+--------------------------------+
|                         EXPR$0 |
+--------------------------------+
|                              a |
+--------------------------------+

-- 不能 有 null
select concat( 'hello', 'world') var1
+--------------------------------+
|                           var1 |
+--------------------------------+
|                     helloworld |
+--------------------------------+

-- 不能 有 null
select concat_ws('|','bigdata', 'flink') var1
+--------------------------------+
|                           var1 |
+--------------------------------+
|                  bigdata|flink |
+--------------------------------+

-- 不能 有 null
select from_base64('') var1, from_base64('SGVsbG8gd29ybGQ=') var2
+--------------------------------+--------------------------------+
|                           var1 |                           var2 |
+--------------------------------+--------------------------------+
|                                |                    Hello world |
+--------------------------------+--------------------------------+

select hash_code('') var1, hash_code('oop') var2
+-------------+-------------+
|        var1 |        var2 |
+-------------+-------------+
|           0 |      110224 |
+-------------+-------------+

-- 返回字符串，每个字转换器的第一个字母大写，其余为小写
select initcap('aBAb') var1
+--------------------------------+
|                           var1 |
+--------------------------------+
|                           Abab |
+--------------------------------+

-- 返回目标字符串在源字符串中的位置，如果在源字符串中未找到目标字符串，则返回0
-- https://www.alibabacloud.com/help/zh/realtime-compute-for-apache-flink/latest/instr?spm=a2c63.p38356.0.0.1d104a9ctDb3KR
select instr('hello word', 'lo') var1, instr('hello word', 'l', -1, 1) var2, instr('hello word', 'l', 3, 2) var3
+-------------+-------------+-------------+
|        var1 |        var2 |        var3 |
+-------------+-------------+-------------+
|           4 |           4 |           4 |
+-------------+-------------+-------------+

select lower('TTy')
+--------------------------------+
|                         EXPR$0 |
+--------------------------------+
|                            tty |
+--------------------------------+

-- https://www.alibabacloud.com/help/zh/realtime-compute-for-apache-flink/latest/lpad?spm=a2c63.p38356.0.0.1d104a9ctDb3KR
-- lpad
select md5('TTy') var1, md5('') var2
+--------------------------------+--------------------------------+
|                           var1 |                           var2 |
+--------------------------------+--------------------------------+
| 3a4124dab524c170a47b1c57a5d... | d41d8cd98f00b204e9800998ecf... |
+--------------------------------+--------------------------------+


select overlay('abcdefg' placing 'hij' from 2 for 2) var1
+--------------------------------+
|                           var1 |
+--------------------------------+
|                       ahijdefg |
+--------------------------------+

-- 返回目标字符串x在被查询字符串y里第一次出现的位置。如果目标字符串x在被查询字符串y中不存在，返回值为0
select position('in' in 'china') var1
+-------------+
|        var1 |
+-------------+
|           3 |
+-------------+


-- https://www.alibabacloud.com/help/zh/realtime-compute-for-apache-flink/latest/regexp-extract?spm=a2c63.p38356.0.0.1d10762am6wcWV

select regexp_extract('foothebar','foo(.*?)(bar)',2) var1
+--------------------------------+
|                           var1 |
+--------------------------------+
|                            bar |
+--------------------------------+


-- 返回以字符串值为str，重复次数为N的新的字符串。如果参数为null时，则返回null。如果重复次数为0或负数，则返回空串。
select repeat('in',3) var1,repeat('in',-1) var1
+--------------------------------+--------------------------------+
|                           var1 |                          var10 |
+--------------------------------+--------------------------------+
|                         ininin |                                |
+--------------------------------+--------------------------------+

select replace('alibaba flink','alibaba','Apache') var1
+--------------------------------+
|                           var1 |
+--------------------------------+
|                   Apache flink |
+--------------------------------+


select reverse('flink') var1,reverse('null') var2
+--------------------------------+--------------------------------+
|                           var1 |                           var2 |
+--------------------------------+--------------------------------+
|                          knilf |                           llun |
+--------------------------------+--------------------------------+

-- 字符串str右端填充若干个字符串pad，直到新的字符串达到指定长度len为止
-- https://www.alibabacloud.com/help/zh/realtime-compute-for-apache-flink/latest/rpad?spm=a2c63.p38356.0.0.1d10762am6wcWV
-- 不能为负数
select rpad('hello',5,'flink') var1,rpad('hello',0,'flink') var2,rpad('hello',10,'flink') var3,rpad('hello',8,'flink') var4
+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
|                           var1 |                           var2 |                           var3 |                           var4 |
+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
|                          hello |                                |                     helloflink |                       hellofli |
+--------------------------------+--------------------------------+--------------------------------+--------------------------------+


select str_to_map('k1=v1,k2=v2')['k1'] var1
+--------------------------------+
|                           var1 |
+--------------------------------+
|                             v1 |
+--------------------------------+


-- ========================================================== date


select current_date as var1


select current_timestamp as var1

-- yyyy-MM-dd 或 yyyy-MM-dd HH:mm:ss
# VARCHAR DATE_ADD(VARCHAR startdate, INT days)
# VARCHAR DATE_ADD(TIMESTAMP time, INT days)
select date_add('2017-09-15 00:00:00',1) as var1,date_add(current_timestamp,1) as var2

select date_sub('2017-09-15 00:00:00',1) as var1

select datediff('2017-10-15 00:00:00','2017-10-15 00:00:00') var1

select date_format('2017-09-15 00:00:00','yyyyMMdd')

select from_unixtime(1505404800) var1,from_unixtime(1505404800,'yyyy-MM-dd') var2

select to_date(100) var1,to_date('2017-09-15') var2,to_date('20170915','yyyy-MM-dd') var3

select unix_timestamp() var1,unix_timestamp('2022-06-10 12:36:00') var2







































