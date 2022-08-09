package com.at;

import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.connector.file.table.PartitionTimeExtractor;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;

import java.time.format.DateTimeFormatterBuilder;

/**
 * @create 2022-08-07
 */
public class Main {

    public static void main(String[] args) throws Exception{

        System.out.println(System.currentTimeMillis());

        DateTimeFormatter TIMESTAMP_FORMATTER = (new DateTimeFormatterBuilder()).appendValue(ChronoField.YEAR, 1, 10, SignStyle.NORMAL).appendLiteral('-').appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL).appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL).optionalStart().appendLiteral(" ").appendValue(ChronoField.HOUR_OF_DAY, 1, 2, SignStyle.NORMAL).appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 1, 2, SignStyle.NORMAL).appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 1, 2, SignStyle.NORMAL).optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd().optionalEnd().toFormatter().withResolverStyle(ResolverStyle.LENIENT);
        DateTimeFormatter DATE_FORMATTER = (new DateTimeFormatterBuilder()).appendValue(ChronoField.YEAR, 1, 10, SignStyle.NORMAL).appendLiteral('-').appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NORMAL).appendLiteral('-').appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NORMAL).toFormatter().withResolverStyle(ResolverStyle.LENIENT);

        System.out.println(TIMESTAMP_FORMATTER);
        System.out.println(DATE_FORMATTER);


        LocalDateTime parse = LocalDateTime.parse("2022-08-07 23:30:17", TIMESTAMP_FORMATTER);

        System.out.println(parse);

//        LocalDateTime parse1 = LocalDateTime.parse("20220807 23:30:17", TIMESTAMP_FORMATTER);
//        System.out.println(parse1);

//        this.extractor = PartitionTimeExtractor.create(
//                cl,
//                (String)conf.get(FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_KIND),
//                (String)conf.get(FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_CLASS),
//                (String)conf.get(FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN),
//                (String)conf.get(FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER));
//        this.watermarkTimeZone = ZoneId.of(conf.getString(FileSystemConnectorOptions.SINK_PARTITION_COMMIT_WATERMARK_TIME_ZONE));


//        Object newInstance = ClassLoader.getSystemClassLoader().loadClass("com.at.ConsumerPartitionTimeExtractor").newInstance();
        Object newInstance = ClassLoader.getSystemClassLoader().loadClass("com.at.writehive.FlinkWriteHiveConsumerPartitionTimeExtractor$ConsumerPartitionTimeExtractor").newInstance();
//        Object newInstance = ClassLoader.getSystemClassLoader().loadClass("com.at.writehive.ConsumerPartitionTimeExtractor").newInstance();

        System.out.println(newInstance);


        System.out.println(System.currentTimeMillis() - 2 * 24 * 3600 * 1000);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(simpleDateFormat.parse("2022-08-09 09:10:00").getTime());

    }

}
