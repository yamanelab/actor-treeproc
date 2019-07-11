package csl.dataproc.actor.remote;

import akka.actor.ActorRef;
import akka.actor.ExtendedActorSystem;
import akka.serialization.ByteBufferSerializer;
import akka.serialization.Serialization;
import akka.serialization.SerializerWithStringManifest;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import csl.dataproc.actor.birch.BirchMessages;
import csl.dataproc.actor.stat.StatMessages;
import csl.dataproc.actor.vfdt.VfdtMessages;
import csl.dataproc.actor.vfdt.replica.ReplicaMessages;
import csl.dataproc.actor.visit.TraversalMessages;
import csl.dataproc.csv.CsvIO;
import csl.dataproc.csv.CsvLine;
import csl.dataproc.csv.CsvStat;
import csl.dataproc.csv.arff.*;
import csl.dataproc.vfdt.SparseDataLine;
import org.objenesis.instantiator.basic.ObjectStreamClassInstantiator;
import org.objenesis.instantiator.sun.UnsafeFactoryInstantiator;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.File;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.*;
import java.nio.*;
import java.nio.charset.Charset;
import java.text.*;
import java.time.*;
import java.time.chrono.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.regex.Pattern;

public class KryoSerializer extends SerializerWithStringManifest implements ByteBufferSerializer {
    protected Pool<Kryo> kryoPool;
    protected Map<Class<?>,Class<?>> specialClasses;
    protected ExtendedActorSystem system;

    public KryoSerializer(ExtendedActorSystem system) {
        this.system = system;
        //Log.set(Log.LEVEL_TRACE);
        kryoPool = new Pool<Kryo>(true, false) {
            @Override
            protected Kryo create() {
                return init();
            }
        };
        specialClasses = new HashMap<>();
        specialClasses.put(Arrays.asList("").getClass(), ArrayList.class);
    }

    public Kryo getKryo() {
        return kryoPool.obtain();
    }

    @Override
    public String manifest(Object o) {
        Kryo kryo = kryoPool.obtain();
        Class cls = o.getClass();
        if (o instanceof ActorRef) {
            System.err.println(cls + " -> ActorRef: " + o);
            cls = ActorRef.class;
        }
        int id = kryo.getRegistration(cls).getId();
        if (id == -1) {
            //System.err.println("KryoSerializer.manifest: " + cls.getName());
            return cls.getName();
        } else {
            return "#" + id;
        }
    }

    public void toBinary(Object o, ByteBuffer byteBuffer) {
        Kryo kryo = kryoPool.obtain();
        write(kryo, new ByteBufferOutput(byteBuffer), o);
    }

    public Object fromBinary(ByteBuffer byteBuffer, String manifest) {
        Kryo kryo = kryoPool.obtain();
        return kryo.readObject(new ByteBufferInput(byteBuffer), getClass(kryo, manifest));
    }

    @Override
    public int identifier() {
        return 4444;
    }


    @Override
    public byte[] toBinary(Object o) {
        Kryo kryo = kryoPool.obtain();
        return write(kryo, o);
    }

    protected byte[] write(Kryo kryo, Object o) {
        try (Output out = new Output(300, Integer.MAX_VALUE)) {
            write(kryo, out, o);
            return out.toBytes();
        }
    }

    protected void write(Kryo kryo, Output out, Object o) {
        kryo.writeObject(out, o);
    }

    @Override
    public Object fromBinary(byte[] bytes, String manifest) {
        Kryo kryo = kryoPool.obtain();
        return kryo.readObject(new Input(bytes), getClass(kryo, manifest));
    }


    public Class<?> getClass(Kryo kryo, String manifest) {
        if (manifest.startsWith("#")) {
            try {
                Class<?> cls = kryo.getRegistration(Integer.valueOf(manifest.substring(1))).getType();
                return specialClasses.getOrDefault(cls, cls);
            } catch (Exception ex) {
                throw new RuntimeException("unsupported manifest " + manifest, ex);
            }
        } else {
            try {
                Class<?> cls = Class.forName(manifest);
                return specialClasses.getOrDefault(cls, cls);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /** registering all serializable types
     * <p>
     *  To add a sub-package:
     *     define your getTypes() in your messages class:
     *      <pre>
     *          static List&lt;Class&gt;?&gt;&gt; getTypes() {
     *               List&lt;Class&lt;?&gt;&gt; list = new ArrayList&lt;&gt;();
     *               list.add(Arrays.asList(
     *                   ... //adding your Serializable classes
     *               ));
     *               return list;
     *          }
     *      </pre>
     *     <pre>
     *         n = register(kryo, MyMessages.getTypes(), n);
     *     </pre>
     *    */
    @SuppressWarnings("unchecked")
    public Kryo init() {
        Kryo kryo = new Kryo();
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

        kryo.addDefaultSerializer(ActorRef.class, new ActorRefSerializer(system));

        int n = 30;
        n = register(kryo, getDataprocTypes(), n);
        n = register(kryo, RemoteManagerMessages.getTypes(), n);
        n = register(kryo, Arrays.asList(StatMessages.class.getClasses()), n);
        n = register(kryo, TraversalMessages.getTypes(), n);
        n = register(kryo, VfdtMessages.getTypes(), n);
        n = register(kryo, ReplicaMessages.getTypes(), n);
        n = register(kryo, BirchMessages.getTypes(), n);

        n = register(kryo, getDefaultSerializerClasses(), n);
        n = registerWithSerializable(kryo, getBaseClasses(), n);
        //n = registerWithSerializable(kryo, getDesktopClasses(), n);
        //n = register(kryo, Serializable.class.getClass().cast(Arrays.asList("").getClass()), n);

        n = registerObjectStream(kryo, EnumMap.class, n);
        n = registerObjectStream(kryo, SimpleTimeZone.class, n);

        for (Class<?> cls : Collections.class.getDeclaredClasses()) {
            if (Serializable.class.isAssignableFrom(cls)) {
                n = registerObjectStream(kryo, (Class<Serializable>) cls, n);
            }
        }

        Registration r = kryo.register(BitSet.class, n);
        r.setSerializer(new BitSetSerializer());
        ++n;

        r = kryo.register(ActorRef.class, n);
        r.setSerializer(new ActorRefSerializer(system));
        ++n;
        return kryo;
    }

    public List<Class<?>> getDefaultSerializerClasses() {
        return Arrays.asList(
                byte[].class,
                char[].class,
                short[].class,
                int[].class,
                long[].class,
                float[].class,
                double[].class,
                boolean[].class,
                String[].class,
                Object[].class,
                KryoSerializable.class,
                BigInteger.class,
                BigDecimal.class,
                Class.class,
                Date.class,
                Enum.class,
                EnumSet.class,
                Currency.class,
                StringBuffer.class,
                StringBuilder.class,
                Collections.EMPTY_LIST.getClass(),
                Collections.EMPTY_MAP.getClass(),
                Collections.EMPTY_SET.getClass(),
                Collections.singletonList(null).getClass(),
                Collections.singletonMap(null,null).getClass(),
                Collections.singleton(null).getClass(),
                TreeSet.class,
                Collection.class,
                TreeMap.class,
                Map.class,
                TimeZone.class,
                Calendar.class,
                Locale.class,
                Charset.class,
                URL.class,
                Arrays.asList().getClass(),
                void.class,
                PriorityQueue.class);
    }

    public List<Class<?>> getBaseClasses() { //java.base
        return Arrays.asList(
                //java.lang
                Boolean.class, Byte.class, Character.class, Double.class, Object.class,
                Float.class, Integer.class, Long.class, Number.class, Object.class, Short.class, Throwable.class, Void.class,

                //java.io
                File.class,

                //java.net
                Inet4Address.class, Inet6Address.class, InetSocketAddress.class, InterfaceAddress.class, URI.class,

                //java.nio
                Buffer.class, ByteBuffer.class, ByteOrder.class, CharBuffer.class, DoubleBuffer.class, FloatBuffer.class,
                IntBuffer.class, LongBuffer.class, ShortBuffer.class,

                //java.text
                AttributedCharacterIterator.Attribute.class, AttributedString.class, ChoiceFormat.class, DateFormat.class,
                DateFormat.Field.class, DateFormatSymbols.class, DecimalFormat.class, DecimalFormatSymbols.class,
                Format.class, Format.Field.class, MessageFormat.class, MessageFormat.Field.class,
                NumberFormat.class, NumberFormat.Field.class, SimpleDateFormat.class,

                //java.time
                Duration.class, Instant.class, LocalDate.class, LocalDateTime.class, LocalTime.class,
                MonthDay.class, OffsetDateTime.class, OffsetTime.class, Period.class, Year.class, YearMonth.class,
                ZonedDateTime.class, ZoneId.class, ZoneOffset.class, DayOfWeek.class, Month.class,

                //java.time.chrono
                HijrahDate.class, HijrahEra.class, JapaneseEra.class, JapaneseDate.class, MinguoEra.class, MinguoDate.class,
                ThaiBuddhistDate.class, ThaiBuddhistEra.class, IsoEra.class,

                //java.util
                ArrayList.class, ArrayDeque.class, BitSet.class, Calendar.class, Currency.class, Date.class,
                EnumMap.class, GregorianCalendar.class, HashMap.class, HashSet.class,  Hashtable.class, IdentityHashMap.class,
                LinkedHashMap.class, LinkedHashSet.class, LinkedList.class, Locale.class,
                Locale.class, Optional.class, OptionalInt.class, OptionalDouble.class, OptionalLong.class,
                PriorityQueue.class, Properties.class, Random.class, SimpleTimeZone.class, Stack.class,
                TreeMap.class, TreeSet.class, UUID.class, Vector.class, WeakHashMap.class,

                //java.util.concurrent
                TimeUnit.class,

                //java.util.concurrent.atomic
                AtomicBoolean.class, AtomicInteger.class, AtomicIntegerArray.class, AtomicLong.class, AtomicLongArray.class,
                AtomicReference.class, AtomicReferenceArray.class,

                //java.util.regex,
                Pattern.class);
    }

//    public List<Class<?>> getDesktopClasses() {
//        return Arrays.asList(Color.class, Point.class, Rectangle.class, Point2D.Float.class, Point2D.Double.class);
//    }

    public static class BitSetSerializer extends Serializer<BitSet> {
        @Override
        public void write(Kryo kryo, Output output, BitSet object) {
            byte[] bs = object.toByteArray();
            output.writeInt(bs.length);
            output.write(bs);
        }

        @Override
        public BitSet read(Kryo kryo, Input input, Class<? extends BitSet> type) {
            int n = input.readInt();
            return BitSet.valueOf(input.readBytes(n));
        }
    }

    public static class ActorRefSerializer extends Serializer<ActorRef> {
        protected ExtendedActorSystem system;

        public ActorRefSerializer(ExtendedActorSystem system) {
            this.system = system;
        }

        @Override
        public void write(Kryo kryo, Output output, ActorRef object) {
            String data = Serialization.serializedActorPath(object);
            output.writeString(data);
        }

        @Override
        public ActorRef read(Kryo kryo, Input input, Class<? extends ActorRef> type) {
            String data = input.readString();
            return system.provider().resolveActorRef(data);
        }
    }

    @SuppressWarnings("unchecked")
    public int register(Kryo kryo, Class<? extends Serializable> cls, int n) {
        Registration r = kryo.register(cls, n);
        try {
            cls.getConstructor();
        } catch (Exception ex) {
            r.setInstantiator(new UnsafeFactoryInstantiator(cls));
        }
        n++;
        return n;
    }

    @SuppressWarnings("unchecked")
    public int registerObjectStream(Kryo kryo, Class<? extends Serializable> cls, int n) {
        Registration r = kryo.register(cls, n);
        try {
            cls.getConstructor();
        } catch (Exception ex) {
            r.setInstantiator(new ObjectStreamClassInstantiator(cls));
            r.setSerializer(new JavaSerializer());
        }
        n++;
        return n;
    }

    public int registerType(Kryo kryo, Class<?> type, int n) {
        kryo.register(type, n);
        n++;
        return n;
    }

    public int register(Kryo kryo, List<Class<?>> types, int n) {
        for (Class<?> t : types) {
            n = registerType(kryo, t, n);
        }
        return n;
    }

    @SuppressWarnings("unchecked")
    public int registerWithSerializable(Kryo kryo, List<Class<?>> types, int n) {
        for (Class<?> t : types) {
            Serializer s = kryo.getDefaultSerializer(t);
            if (Serializable.class.isAssignableFrom(t) && s instanceof FieldSerializer) {
                Registration r = kryo.register(t, n);
                r.setInstantiator(new ObjectStreamClassInstantiator(t));
                r.setSerializer(new JavaSerializer());
            } else {
                n = registerType(kryo, t, n);
            }
            n++;
        }
        return n;
    }

    public static List<Class<?>> getDataprocTypes() {
        return Arrays.asList(
                ArffAttribute.class,
                ArffHeader.class,
                CsvStat.ColumnStat.class,
                CsvIO.CsvSettings.class,
                CsvLine.class,
                ArffLine.class,
                ArffSparseLine.class,
                CsvSparseColumn.class,
                ArffAttributeType.DateAttributeType.class,
                ArffAttributeType.EnumAttributeType.class,
                ArffAttributeType.RelationalAttributeType.class,
                ArffAttributeType.SimpleAttributeType.class,
                SparseDataLine.SparseDataColumn.class,
                SparseDataLine.SparseDataColumnInteger.class,
                SparseDataLine.SparseDataColumnReal.class,
                SparseDataLine.SparseDataColumnString.class,
                SparseDataLine.class
        );
    }
}
