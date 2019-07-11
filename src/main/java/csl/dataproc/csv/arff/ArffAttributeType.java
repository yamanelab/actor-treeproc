package csl.dataproc.csv.arff;

import csl.dataproc.csv.ArffReader;

import java.io.Serializable;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public interface ArffAttributeType {
    char ATTRIBUTE_KIND_INTEGER = 'i';
    char ATTRIBUTE_KIND_REAL = 'r';
    char ATTRIBUTE_KIND_STRING = 's';
    char ATTRIBUTE_KIND_RELATIONAL = 'R';
    char ATTRIBUTE_KIND_ENUM = 'e';
    char ATTRIBUTE_KIND_DATE = 'd';

    char getKind();

    enum SimpleAttributeType implements ArffAttributeType, Serializable {
        Numeric(ATTRIBUTE_KIND_REAL),
        Integer(ATTRIBUTE_KIND_INTEGER),
        Real(ATTRIBUTE_KIND_REAL),
        String(ATTRIBUTE_KIND_STRING);

        static final long serialVersionUID = 1L;

        private char c;

        SimpleAttributeType() {}

        SimpleAttributeType(char c) {
            this.c = c;
        }

        @Override
        public char getKind() {
            return c;
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    class RelationalAttributeType implements ArffAttributeType, Serializable {
        protected String name;
        protected List<ArffAttribute> attributes;

        static final long serialVersionUID = 1L;

        public RelationalAttributeType() {
            this("", new ArrayList<>());
        }

        public RelationalAttributeType(String name, List<ArffAttribute> attributes) {
            this.name = name;
            this.attributes = attributes;
        }

        public List<ArffAttribute> getAttributes() {
            return attributes;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return attributes.stream()
                    .map(ArffAttribute::toString)
                    .map(s -> String.join("   \n", s.split("\\n")))
                    .collect(Collectors.joining("\n", "relational\n", "@end " + ArffReader.quoteString(name)));
        }

        @Override
        public char getKind() {
            return ATTRIBUTE_KIND_RELATIONAL;
        }
    }

    class DateAttributeType implements ArffAttributeType, Serializable {
        protected DateTimeFormatter formatter;

        static final long serialVersionUID = 1L;

        public DateAttributeType() {
            this(null);
        }

        public DateAttributeType(DateTimeFormatter formatter) {
            this.formatter = formatter;
            if (formatter == null) {
                this.formatter = DateTimeFormatter.ISO_DATE_TIME;
            }
        }

        public DateTimeFormatter getFormatter() {
            return formatter;
        }

        @Override
        public String toString() {
            return "date " + ArffReader.quoteString(formatter.toString());
        }

        @Override
        public char getKind() {
            return ATTRIBUTE_KIND_DATE;
        }

        /** return epochSecond - 1 if failed to parse.
         *  default zone: UTC, default time: 00:00, default date: epoch day (1971.01.01)  */
        public Instant parse(String data) {
            try {
                TemporalAccessor t = formatter.parse(data);

                ZoneId zone = t.query(TemporalQueries.zone());
                if (zone == null) {
                    zone = ZoneOffset.UTC;
                }
                LocalTime time = t.query(TemporalQueries.localTime());
                if (time == null) {
                    time = LocalTime.MIN;
                }
                LocalDate date = t.query(TemporalQueries.localDate());
                if (date == null) {
                    date = LocalDate.ofEpochDay(0);
                }
                return ZonedDateTime.of(date, time, zone).toInstant();
            } catch (Exception ex) {
                return Instant.ofEpochSecond(-1);
            }
        }
    }

    class EnumAttributeType implements ArffAttributeType, Serializable {
        protected List<String> values;

        static final long serialVersionUID = 1L;

        public EnumAttributeType() {
            this(new ArrayList<>());
        }

        public EnumAttributeType(List<String> values) {
            this.values = values;
        }

        public List<String> getValues() {
            return values;
        }

        @Override
        public String toString() {
            return values.stream()
                            .map(ArffReader::quoteString)
                            .collect(Collectors.joining(",", "{", "}"));
        }

        @Override
        public char getKind() {
            return ATTRIBUTE_KIND_ENUM;
        }
    }
}
