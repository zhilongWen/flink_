package com.at;

import org.apache.flink.connector.file.table.PartitionTimeExtractor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * @create 2022-08-08
 */
public class ConsumerPartitionTimeExtractor implements PartitionTimeExtractor {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss", Locale.CHINA);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINA);

//    @Nullable
    private final String extractorPattern = "$dt $hm:$mm:00";
//    @Nullable
    private final String formatterPattern = null;

    public ConsumerPartitionTimeExtractor(){};

//    public ConsumerPartitionTimeExtractor(@Nullable String extractorPattern, @Nullable String formatterPattern) {
//            this.extractorPattern = extractorPattern;
//        this.extractorPattern = "$dt $hm:$mm:00";
//        this.formatterPattern = formatterPattern;
//    }

    @Override
    public LocalDateTime extract(List<String> partitionKeys, List<String> partitionValues) {
        String timestampString;
        if (this.extractorPattern == null) {
            timestampString = (String) partitionValues.get(0);
        } else {
            timestampString = this.extractorPattern;

            for (int i = 0; i < partitionKeys.size(); ++i) {
                timestampString = timestampString.replaceAll("\\$" + (String) partitionKeys.get(i), (String) partitionValues.get(i));
            }
        }

        return toLocalDateTime(timestampString, this.formatterPattern);
    }

    public static LocalDateTime toLocalDateTime(String timestampString, @Nullable String formatterPattern) {
        if (formatterPattern == null) {
            return toLocalDateTimeDefault(timestampString);
        } else {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern((String) Objects.requireNonNull(formatterPattern), Locale.ROOT);

            try {
                return LocalDateTime.parse(timestampString, (DateTimeFormatter) Objects.requireNonNull(dateTimeFormatter));
            } catch (DateTimeParseException var4) {
                return LocalDateTime.of(LocalDate.parse(timestampString, (DateTimeFormatter) Objects.requireNonNull(dateTimeFormatter)), LocalTime.MIDNIGHT);
            }
        }
    }

    public static LocalDateTime toLocalDateTimeDefault(String timestampString) {
        try {
            return LocalDateTime.parse(timestampString, TIMESTAMP_FORMATTER);
        } catch (DateTimeParseException var2) {
            return LocalDateTime.of(LocalDate.parse(timestampString, DATE_FORMATTER), LocalTime.MIDNIGHT);
        }
    }

}
