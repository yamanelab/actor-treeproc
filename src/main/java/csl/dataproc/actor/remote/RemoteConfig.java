package csl.dataproc.actor.remote;

import akka.actor.ActorSystem;
import akka.dispatch.UnboundedControlAwareMailbox;
import akka.serialization.JavaSerializer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import csl.dataproc.actor.stat.StatMailbox;
import csl.dataproc.actor.stat.StatMailboxSimple;
import csl.dataproc.actor.stat.StatSystem;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * a general remote config with arguments parsing
 * <p>
 * use {@link #create(List)} for parsing command line arguments.
 *  it returns an instance of the class,
 *  and it holds arguments that were not handled in the class as {@link #restArgs}.
 * <ul>
 *     <li> {@link #initConfig(Config)}: set up the akka config file {@link Config}.
 *
 *          <ul>
 *              <li>
 *                  if {@link #config} is not provided,
 *                  create a new {@link Config} object by {@link ConfigFactory#load()},
 *                  or create from <code>--conf '{conf-data...}'</code> or <code>--conf-file file.conf</code>
 *              </li>
 *
 *              <li>
 *                  {@link #getConfigString()} : additional config data constructed from arguments
 *              </li>
 *
 *              <li> if {@link #precedeConfFile}, then the former config is preceded.
 *                   if not, the latter preceded (default). </li>
 *          </ul>
 *      </li>
 *     <li> system creation: {@link #createSystem()} returns a new {@link ActorSystem}
 *            with the {@link #config}.
 *            the {@link #name} can be set by <code>--name n</code>
 *            (default is {@link #SYSTEM_NAME}: "dataproc")
 *            </li>
 *
 *     <li> using {@link StatSystem}: {@link #stat},
 *         if  <code>--stat</code> is supplied.
 *          {@link #setStat()} is called before the system creation.
 *           In the method, it calls {@link StatSystem#system()},
 *           and then a {@link StatSystem#} will be set and enabled.
 *          <p>
 *         if <code>--stat-dir dir</code> is supplied, it sets {@link StatSystem#setDir(Path)}.
 *         if <code>--stat-interval PT...</code> is supplied, it sets {@link StatSystem#setInterval(Duration)}.
 *             {@link Duration#parse(CharSequence)} makes the interval.
 *
 *          <p>
 *              {@link #CONF_STAT_MAILBOX} is set:
 *                 "stat-mailbox.mailbox-type" to the full class name of {@link StatMailbox}.
 *                  This will be used in {@link RemoteManager} actor creating if stat is enabled.
 *     </li>
 *
 *     <li> unique system id of this instance, {@link #getSystemId()}:
 *         At the first time, it calls {@link #getSystemIdName()}.
 *           This returns {@link #name}@{@link #getHostName()}:{@link #port}.
 *           The host name can be manually set by <code>--host h</code>.
 *             if {@link #host} is not set or "-", {@link InetAddress#getLocalHost()} is used.
 *
 *          <p>
 *          The idName is converted to a long {@link #systemId} by {@link StatSystem#getSystemIdFromName(String)}.
 *         <p>
 *           The id can be manually set by <code>--sys-id N</code>
 *     </li>
 *
 *     <li> round robin actors, {@link #count}: set by <code>--count N</code>,
 *            supplied to {@link RemoteManager}.
 *            Each cluster node can create the count N actors for each round.
 *     </li>
 *
 *     <li> remoting leader or follower, {@link #leader}: set by <code>--leader</code> (default is false),
 *          supplied to {@link RemoteManager}.
 *          A follower can join to the leader by <code>--join addr</code>.
 *          <p>
 *              if <code>--test</code>, {@link RemoteManager} will test connections with logging.
 *     </li>
 *
 *     <li> remoting mode {@link Provider} {@link #provider}:
 *        <code>--artery</code>, <code>--netty</code> (default),
 *          <code>--local</code>, or <code>--cluster</code>.
 *           {@link #CONF_PROVIDER}:"akka.actor.provider" is set by {@link #getProviderString()}.
 *
 *         <ul>
 *          <li>
 *            {@link Provider#RemoteArtery}:  {@link #isArtery()} returns true.
 *            {@link #CONF_PROVIDER}:"akka.actor.provider" to "remote".
 *            {@link #CONF_PORT_ARTERY}:"akka.remote.artery.canonical.port" to {@link #port},
 *            {@link #CONF_HOST_ARTERY}:"akka.remote.artery.canonical.hostname" to {@link #getHostName()},
 *            and {@link #CONF_ARTERY_ENABLED}:"akka.remote.artery.enabled" (set to "on").
 *
 *            <b>artery uses UDP, it might lose some messages. seems to be not recommended</b>
 *          </li>
 *          <li>
 *            {@link Provider#RemoteNetty}: {@link #isNetty()} returns true.
 *            {@link #CONF_PROVIDER}:"akka.actor.provider" to "remote".
 *            {@link #CONF_PORT_NETTY}:"akka.remote.netty.tcp.port" to {@link #port},
 *            {@link #CONF_HOST_NETTY}:"akka.remote.netty.tcp.hostname" to {@link #getHostName()},
 *            and {@link #CONF_TRANSPORT}:"akka.remote.enabled-transports" to ["akka.remote.netty.tcp"].
 *         </li>
 *         <li>
 *             {@link Provider#Cluster}:
 *                {@link #CONF_PROVIDER}:"akka.actor.provider" to "cluster".
 *         </li>
 *         <li>
 *              {@link Provider#Local}:
 *                {@link #CONF_PROVIDER}:"akka.actor.provider" to "local".
 *         </li>
 *         </ul>
 *     </li>
 *
 *     <li> control mailbox, {@link #CONF_CONTROL_MAILBOX}:
 *              "control-mailbox.mailbox-type" is set to the full class name of {@link StatMailbox.EnvelopePriorityMailboxType}.
 *           <p>
 *             The mail box will be set to an actor created from {@link RemoteManager} if stat is disabled or
 *                 calling <code>props.withMailbox({@link #CONF_CONTROL_MAILBOX})</code>.
 *     </li>
 *     <li> pinned dispatcher, {@link #CONF_PINNED_DISPATCHER}:
 *              "pinned-dispatcher.executor" is set to "thread-pool-executor",
 *              and "pinned-dispatcher.type" is set to "PinnedDispatcher".
 *     </li>
 *
 *     <li> serialization settings by {@link #getConfigStringSerialization()}:
 *            <code>--no-kryo</code> set {@link #kryo} (default is true).
 *            if {@link #kryo} is set,
 *               "akka.actor.enable-additional-serialization-bindings" to "on",
 *                  "akka.actor.serializers.kryo" to the full class name of {@link KryoSerializer},
 *                 "akka.actor.serialization-bindings" to { "java.io.Serializable" = "kryo" },
 *                 and "akka.actor.serialization-identifiers" to { "csl.dataproc.actor.remote.KryoSerializer" = "4444" }.
 *
 *               also if {@link #kryo} and {@link #provider} is artery,
 *                  "akka.actor.allow-java-serialization" to "off",
 *                  "akka.actor.serializers.java" to the full class name of {@link JavaSerializer}.
 *
 *
 *     </li>
 *
 *     <li> shutdown flag: <code>--shutdown-cluster</code> :
 *     your application can check the flag and send Shutdown message to {@link RemoteManager}
 *     </li>
 * </ul>
 *
 * <p>
 *     {@link #toJsonByIntrospection(Object)}: converting objects to pure-json objects.
 * </p>
 *
 *  example:
 * <pre>
 *     void start(String[] args) {
 *          RemoteConfig conf = RemoteConfig.create(Arrays.asList(args));
 *          ActorSystem system = conf.createSystem();
 *
 *          ActorRef manager = RemoteManager.createManager(system, conf);
 *
 *          List&lt;String&gt; restArgs = conf.restArgs;
 *          MyConfig myConf = new MyConfig(); //your original configuration as an instance of SharedConfig
 *          ... //parse your original arguments for restArgs
 *
 *          manager.tell(new RemoteManagerMessages.SetConfig(myConf));
 *            //you can deliver your config to the manager node,
 *            //  and then, the config will be called arriveAtRemoteNode().
 *            //  Also the manger will automatically deliver the config to all other nodes
 *          Thread.sleep(1000); //wait for the setting
 *
 *          ref = manager.create(MyActor.class, MyActor::new); //obtains your actor
 *          ...
 *     }
 *
 * </pre>
 * */
public class RemoteConfig {
    public String name = SYSTEM_NAME;
    public int port = 0;
    public long count = 1000;
    public List<String> joins = new ArrayList<>();
    public boolean leader = false;
    public boolean shutdownCluster = false;

    public String host;
    public Provider provider = Provider.RemoteNetty;

    public boolean stat;
    public Duration statInterval = Duration.ofSeconds(10);
    public String statDir;

    public boolean kryo = true;
    public String kryoSerializer = KryoSerializer.class.getName();

    public boolean precedeConfFile = false;

    public boolean test;

    public long systemId = -1;

    public transient Config config;

    public List<String> restArgs = Collections.emptyList();

    public boolean queueSimple;

    public static RemoteConfig create(List<String> args) {
        RemoteConfig c = new RemoteConfig();
        c.parseArgs(args);
        return c;
    }

    public List<String> parseArgs(List<String> args) {
        List<String> rest = new ArrayList<>();
        if (config == null) {
            config = ConfigFactory.load();
        }
        for (int i = 0; i < args.size(); ++i) {
            String arg = args.get(i);
            if (arg.equals("--name")) { //--name sysName
                ++i;
                name = args.get(i);
            } else if (arg.equals("--sys-id")) {
                ++i;
                systemId = Long.valueOf(args.get(i));
            } else if (arg.equals("--host")) { //--host hostName
                ++i;
                host = args.get(i);
            } else if (arg.equals("--port")) { //--port P
                ++i;
                port = Integer.valueOf(args.get(i));
            } else if (arg.equals("--leader")) {
                leader = true;
            } else if (arg.equals("--count")) { //--count N
                ++i;
                count = Long.valueOf(args.get(i));
            } else if (arg.equals("--conf")) { //--conf resourceBaseName
                ++i;
                config = wrapConfig(ConfigFactory.load(args.get(i)), config);
            } else if (arg.equals("--conf-file")) { //--conf-file path.conf
                ++i;
                config = wrapConfig(ConfigFactory.parseFile(new File(args.get(i))), config);
            } else if (arg.equals("--join")) { //--join akka://dataproc@masterHost:masterPort
                ++i;
                joins.add(args.get(i));
            } else if (arg.equals("--stat")) {
                stat = true;
            } else if (arg.equals("--stat-interval")) {
                ++i;
                statInterval = Duration.parse(args.get(i));
            } else if (arg.equals("--stat-dir")) {
                ++i;
                statDir = args.get(i);
            } else if (arg.equals("--no-kryo")) {
                kryo = false;
            } else if (arg.equals("--artery")) {
                provider = Provider.RemoteArtery;
            } else if (arg.equals("--local")) {
                provider = Provider.Local;
            } else if (arg.equals("--netty")) {
                provider = Provider.RemoteNetty;
            } else if (arg.equals("--cluster")) {
                provider = Provider.Cluster;
            } else if (arg.equals("--precede-conf")) {
                precedeConfFile = true;
            } else if (arg.equals("--test-connection")) {
                test = true;
            } else if (arg.equals("--shutdown-cluster")) {
                shutdownCluster = true;
            } else if (arg.equals("--queue-simple")) {
                queueSimple = true;
            } else if (arg.equals("-h") || arg.equals("--help")) {
                rest.add("--help");
                showHelp();
                break;
            } else {
                rest.add(arg);
            }
        }
        updateJoins();
        config = initConfig(config);
        restArgs = rest;
        return rest;
    }

    public void showHelp() {
        String helpStr = String.join("\n",
                "Remote options:",
                "  --sys-id <long> : explicit system id",
                "  --name <systemName> : default is " + name,
                "  --host <hostname> : omission or - means the canonical local host",
                "  --port <port> : 0 means dynamic port",
                "  --leader : make this process leader",
                "  --count <N> : the round robin actor count. default is " + count,
                "  --conf <resourceBaseName> : the parameter for ConfigFactory.load",
                "  --conf-file <path> : read from the path",
                "  --precede-conf: mainly use specified config file with fallback to arguments based config",
                "  --join <addr> : join to the leader as a cluster node. e.g. akka.tcp://" + name + "@host:port or host:port",
                "  --stat: enable StatSystem",
                "  --stat-dir <path>",
                "  --stat-interval <duration> : for Duration.parse. e.g. PT0.5S. default is " + statInterval,
                "  --no-kryo",
                "  --netty   : remoting mode with netty tcp (default)",
                "  --artery  : remoting mode with artery ",
                "  --cluster : remoting mode with cluster and artery",
                "  --local   : local mode",
                "  --test-connection  : test connection",
                "  --shutdown-cluster : send shutdown cluster nodes if leader",
                "  --queue-simple ");
        System.out.println(helpStr);
    }

    public enum Provider {
        Local, RemoteArtery, RemoteNetty, Cluster
    }

    public static String SYSTEM_NAME = "dataproc";

    public static String CONF_PORT_ARTERY = "akka.remote.artery.canonical.port";
    public static String CONF_PORT_NETTY = "akka.remote.netty.tcp.port";

    public static String CONF_HOST_ARTERY = "akka.remote.artery.canonical.hostname";
    public static String CONF_HOST_NETTY = "akka.remote.netty.tcp.hostname";

    public static String CONF_RECEIVE_SIZE_NETTY = "akka.remote.netty.tcp.receive-buffer-size"; //default: 256k
    public static String CONF_SEND_SIZE_NETTY = "akka.remote.netty.tcp.send-buffer-size"; //default 256k
    public static String CONF_FRAME_SIZE_NETTY = "akka.remote.netty.tcp.maximum-frame-size"; //default: 128K
    public static String CONF_BACKLOG_NETTY = "akka.remote.netty.tcp.backlog"; //default: 4096

    public static String CONF_SERVER_SOCKET_POOL_MAX_NETTY = "akka.remote.netty.tcp.server-socket-worker-pool.pool-size-max"; //default: 2
    public static String CONF_CLIENT_SOCKET_POOL_MAX_NETTY = "akka.remote.netty.tcp.client-socket-worker-pool.pool-size-max"; //default: 2

    public static String CONF_TRANSPORT = "akka.remote.enabled-transports";
    public static String CONF_ARTERY_ENABLED = "akka.remote.artery.enabled";

    public static String CONF_PROVIDER = "akka.actor.provider";
    public static String CONF_DEFAULT_MAILBOX_TYPE = "akka.actor.default-mailbox.mailbox-type";

    public static String CONF_STAT_MAILBOX = "stat-mailbox";
    public static String CONF_CONTROL_MAILBOX = "control-mailbox";
    public static String CONF_PINNED_DISPATCHER = "pinned-dispatcher";

    public ActorSystem createSystem() {
        if (stat) {
            setStat();
        }
        return ActorSystem.create(name, config);
    }

    public void updateJoins() {
        joins = joins.stream()
                .map(this::updateJoin)
                .collect(Collectors.toList());
    }
    public String updateJoin(String addr) {
        int si;
        String scheme;
        if ((si = addr.indexOf("://")) != -1) {
            scheme = addr.substring(0, si);
            si += "://".length();
        } else {
            scheme = isArtery() ? "akka" : "akka.tcp";
            si = 0;
        }
        int ni;
        String name;
        if ((ni = addr.indexOf("@", si)) != -1) {
            name = addr.substring(si, ni);
            ni += "@".length();
        } else {
            name = this.name;
            ni = si;
        }
        int hi;
        String host;
        String port;
        if ((hi = addr.indexOf(":", ni)) != -1) {
            host = addr.substring(ni, hi);
            port = addr.substring(hi + ":".length());
        } else {
            host = this.getHostName();
            port = addr.substring(ni);
        }

        return scheme + "://" + name + "@" + host + ":" + port;
    }

    public Config initConfig(Config config) {
        String s = getConfigString();
        System.err.println("[" + getSystemIdName() + "] : config: \n    " + String.join("\n    ", s.split("\\n")));
        Config genConf = ConfigFactory.parseString(s);
        return precedeConfFile ? wrapConfig(config, genConf) : wrapConfig(genConf, config);
    }

    public Config wrapConfig(Config c, Config config) {
        if (c == null) {
            return config;
        } else {
            if (config != null) {
                return c.withFallback(config);
            } else {
                return c.withFallback(ConfigFactory.defaultApplication());
            }
        }
    }

    public String getConfigString() {
        return  confEntry(!getProviderString().equals(""),
                        CONF_PROVIDER, getProviderString()) +
                //confEntry(stat,
                        //CONF_DEFAULT_MAILBOX_TYPE, confClassValue(StatMailbox.class)) +
                confEntry(isArtery() && port >= 0,
                        CONF_PORT_ARTERY, Integer.toString(port)) +
                confEntry(isArtery(),
                        CONF_HOST_ARTERY, getHostName()) +
                confEntry(isNetty() && port >= 0,
                        CONF_PORT_NETTY, Integer.toString(port)) +
                confEntry(isNetty(),
                        CONF_HOST_NETTY, getHostName()) +
                confEntry(isNetty(),
                        CONF_TRANSPORT, "[\"akka.remote.netty.tcp\"]") +
                confEntry(isNetty(),
                        CONF_RECEIVE_SIZE_NETTY, "96000000b") +
                confEntry(isNetty(),
                        CONF_SEND_SIZE_NETTY,    "96000000b") +//96MB
                confEntry(isNetty(),
                        CONF_FRAME_SIZE_NETTY,   "24000000b") + //24MB
                confEntry(isNetty(),
                        CONF_SERVER_SOCKET_POOL_MAX_NETTY,   "24") +
                confEntry(isNetty(),
                        CONF_CLIENT_SOCKET_POOL_MAX_NETTY,   "24") + 
                confEntry(isNetty(),
                        CONF_BACKLOG_NETTY,   "12000") + //x3
                confEntry(isArtery(),
                        CONF_ARTERY_ENABLED, "on") +
                getConfigStringSerialization() +
                confEntry(!queueSimple && stat, CONF_STAT_MAILBOX + ".mailbox-type", confClassValue(StatMailbox.class)) +
                confEntry( queueSimple && stat, CONF_STAT_MAILBOX + ".mailbox-type", confClassValue(StatMailboxSimple.class)) +
                confEntry(!queueSimple, CONF_CONTROL_MAILBOX + ".mailbox-type", confClassValue(StatMailbox.EnvelopePriorityMailboxType.class)) +
                confEntry( queueSimple, CONF_CONTROL_MAILBOX + ".mailbox-type", confClassValue(UnboundedControlAwareMailbox.class)) +
                confEntry(true, CONF_PINNED_DISPATCHER + ".executor", "\"thread-pool-executor\"") +
                confEntry(true, CONF_PINNED_DISPATCHER + ".type", "PinnedDispatcher") +
                "";
    }

    public String getConfigStringSerialization() {
        return  confEntry(kryo, "akka.actor.enable-additional-serialization-bindings", "on") +
                confEntry(kryo && provider.equals(Provider.RemoteArtery), "akka.actor.allow-java-serialization", "off") +
                confEntry(kryo, "akka.actor.serializers.kryo",
                        confClassValue(kryoSerializer)) +
                confEntry(kryo && !provider.equals(Provider.RemoteArtery), "akka.actor.serializers.java",
                        confClassValue(JavaSerializer.class)) +
                confEntryKeyValue(kryo, "akka.actor.serialization-bindings",
                        confClassValue(Serializable.class), "kryo") +
                confEntryKeyValue(kryo, "akka.actor.serialization-identifiers",
                        confClassValue(kryoSerializer), "4444");
    }

    public boolean isArtery() {
        return provider.equals(Provider.RemoteArtery) ||
                provider.equals(Provider.Cluster);
    }

    public boolean isNetty() {
        return provider.equals(Provider.RemoteNetty);
    }

    public boolean isRemote() {
        return !provider.equals(Provider.Local);
    }

    public String getProviderString() {
        switch (provider) {
            case Local:
                return "local";
            case RemoteArtery:
            case RemoteNetty:
                return "remote";
            case Cluster:
                return "cluster";
            default:
                return "";
        }
    }

    public String getHostName() {
        try {
            return (host == null || host.equals("-") || host.isEmpty()) ?
                    InetAddress.getLocalHost().getCanonicalHostName() :
                    host;
        } catch (Exception ex) {
            return "127.0.0.1";
        }
    }

    protected String confEntry(boolean enable, String key, String value) {
        return enable ? confEntry(key, value) : "";
    }

    protected String confEntryKeyValue(boolean enable, String key, String eKey, String eValue) {
        return enable ? key + " { " + eKey + " = " + eValue + " }\n" : "";
    }

    protected String confEntry(String key, String value) {
        return key + "=" + value + "\n";
    }

    protected String confClassValue(Class<?> cls) {
        return confClassValue(cls.getName());
    }

    protected String confClassValue(String name) {
        return "\"" + name + "\"";
    }

    public void setStat() {
        StatSystem s = StatSystem.system();
        s.setEnabled(true);
        if (statDir != null) {
            s.setDir(Paths.get(statDir));
        }
        s.setInterval(statInterval);
    }

    public long getSystemId() {
        if (systemId == -1) {
            String idName = getSystemIdName();
            System.err.println("systemIdName: " + idName);
            systemId = StatSystem.system().getSystemIdFromName(idName);
        }
        return systemId;
    }

    public String getSystemIdName() {
        return name + "@" + getHostName() + ":" + port;
    }

    /**
     * convert an object to a JSON native object.
     * @param obj
     *    <ul>
     *         <li>
     *             {@link String}, {@link Number} and {@link Boolean}: JSON native object as is
     *         </li>
     *         <li>
     *             {@link Character}: {@link Character#toString()}.
     *         </li>
     *         <li>
     *             {@link Instant}: {@link LocalDateTime#toString()} with {@link ZoneId#systemDefault()}.
     *         </li>
     *         <li>
     *             {@link Duration}: {@link Duration#toString()}.
     *         </li>
     *         <li>
     *             {@link Enum}: {@link Enum#name()}
     *         </li>
     *         <li>
     *             {@link List}, {@link Map}: recursively converted.
     *              for keys of map, {@link Object#toString()} is used.
     *         </li>
     *         <li>
     *             otherwise, obtains public fields by {@link Class#getFields()},
     *               and returns {@link HashMap}
     *               with constructing entries from the fields that satisfy {@link #isJsonType(Type)}.
     *               the name of each field is a key and values are recursively converted.
     *         </li>
     *     </ul>
     * @return a JSON native object:
     *    {@link Map}, {@link List}, {@link String}, {@link Number} and {@link Boolean}
     */
    @SuppressWarnings("unchecked")
    public static Object toJsonByIntrospection(Object obj) {
        if (obj == null) {
            return null;
        } else {
            Class<?> cls = obj.getClass();
            if (obj instanceof String ||
                    obj instanceof Number ||
                    obj instanceof Boolean) {
                return obj;
            } else if (obj instanceof Character) {
                return obj.toString();
            } else if (obj instanceof Instant) {
                return LocalDateTime.ofInstant((Instant) obj, ZoneId.systemDefault()).toString();
            } else if (obj instanceof Duration) {
                return obj.toString();
            } else if (obj instanceof List<?>) {
                return ((List<Object>) obj).stream()
                        .map(RemoteConfig::toJsonByIntrospection)
                        .collect(Collectors.toList());
            } else if (obj instanceof Map<?, ?>) {
                Map<String, Object> json = new HashMap<>();
                //no null key
                ((Map<Object, Object>) obj).forEach((k, v) ->
                        json.put(k.toString(), toJsonByIntrospection(v)));
                return json;
            } else if (obj instanceof Enum<?>) {
                return ((Enum<?>) obj).name();
            } else {
                Map<String, Object> json = new HashMap<>();
                for (Field f : cls.getFields()) { //public fields
                    String fldName = f.getName();
                    if (!Modifier.isTransient(f.getModifiers()) && !Modifier.isStatic(f.getModifiers()) &&
                            isJsonType(f.getGenericType())) {
                        Object o;
                        try {
                            o = f.get(obj);
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                        json.put(fldName, toJsonByIntrospection(o));
                    }
                }
                return json;
            }
        }
    }

    /**
     * check the type for converting JSON
     * @param type  tested type
     *  For List and Map, the type must be {@link ParameterizedType}.
     *  Primitive types are allowed.
     * @return true if {@link #toJsonByIntrospection(Object)} can handle, but
     *  false for Object.
     */
    public static boolean isJsonType(Type type) {
        if (type instanceof Class<?>) {
            Class<?> clsType = (Class<?>) type;
            return Number.class.isAssignableFrom(clsType) ||
                    Boolean.class.isAssignableFrom(clsType) ||
                    clsType.isPrimitive() ||
                    Duration.class.isAssignableFrom(clsType) ||
                    Instant.class.isAssignableFrom(clsType) ||
                    String.class.isAssignableFrom(clsType) ||
                    Enum.class.isAssignableFrom(clsType);
        } else if (type instanceof ParameterizedType) {
            Type raw = ((ParameterizedType) type).getRawType();
            return raw instanceof Class<?> &&
                    (((List.class.isAssignableFrom((Class<?>) raw) ||
                       Map.class.isAssignableFrom((Class<?>) raw)) &&
                       Arrays.stream(((ParameterizedType) type).getActualTypeArguments())
                        .allMatch(RemoteConfig::isJsonType))  ||
                     Enum.class.isAssignableFrom((Class<?>) raw));
        } else {
            return false;
        }
    }
}
