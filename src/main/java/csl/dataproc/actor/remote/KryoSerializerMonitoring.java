package csl.dataproc.actor.remote;

import akka.actor.ExtendedActorSystem;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class KryoSerializerMonitoring extends KryoSerializer  {
    public KryoSerializerMonitoring(ExtendedActorSystem system) {
        super(system);
    }

    @Override
    public byte[] toBinary(Object o) {
        byte[] data = super.toBinary(o);
        report.report(o, data == null ? 0 : data.length);
        return data;
    }

    @Override
    public void toBinary(Object o, ByteBuffer byteBuffer) {
        super.toBinary(o, byteBuffer);
        int total = byteBuffer.remaining();
        report.report(o, total);
    }

    //the serializer is built for each thread
    SerializeReport report = new SerializeReport();
    static SerializeReport globalReport = new SerializeReport();

    public static class SerializeReport {
        Instant lastTime = Instant.now();
        Map<String, SerializeStat> statMap = new HashMap<>();

        public void report(Object o, int size) {
            String name;
            if (o != null) {
                name = o.getClass().getName();
            } else {
                name = "<null>";
            }

            statMap.computeIfAbsent(name, SerializeStat::new)
                    .add(size);
            Instant now = Instant.now();
            if (Duration.between(lastTime, now).compareTo(Duration.ofMinutes(1)) > 0) {
                globalReport.logStat(statMap);
                lastTime = now;
            }
        }

        public synchronized void logStat(Map<String,SerializeStat> stat) {
            stat.forEach((k,v) ->
                    statMap.computeIfAbsent(k, SerializeStat::new).add(v));
            System.err.println("# serialize-stat: types=" + statMap.size() + " ------------------");
            statMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(
                            Comparator.comparing(SerializeStat::getTotal)))
                    .map(Map.Entry::getValue)
                    .forEach(System.err::println);
        }
    }

    public static class SerializeStat {
        String className;
        long count;
        long total;
        int last;

        public SerializeStat(String name) {
            this.className = name;
        }

        public void add(SerializeStat s) {
            count += s.count;
            total += s.total;
            last = s.last;
        }

        public void add(int size) {
            count++;
            total += size;
            last = size;
        }

        public long getTotal() {
            return total;
        }

        @Override
        public String toString() {
            return String.format("# serialize-stat count=%,12d  total=%,12d bytes, last=%,12d bytes : #%s",
                    count, total, last, className);
        }
    }
}
