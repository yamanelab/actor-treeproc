package csl.dataproc.actor.stat;

import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.DeadLetter;
import csl.dataproc.dot.DotAttribute;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.dot.DotNode;
import csl.dataproc.tgls.util.JsonReader;
import csl.dataproc.tgls.util.JsonWriter;
import scala.concurrent.duration.FiniteDuration;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RemoteManager will automatically set the instance and interval task by <code>--stat</code> option of RemoteConfig.
 */
public class StatVm {
    public static String vmTotalNameFormat = "vmt%d.json";
    public static String vmDurationNameFormat = "vmd%d.jsonp";
    public static Pattern vmDurationNamePattern = Pattern.compile("vmd(.*?)\\.jsonp");
    protected long id;
    public StatRuntime statRuntime;
    public StatRuntime totalRuntime;

    public static StatVm instance;

    public Cancellable intervalTask;

    public void saveRuntime(ActorSystem system, Duration interval) {
        try {
            StatSystem statSystem = StatSystem.system();
            Path dir = statSystem.getDir();
            if (dir != null) {
                Files.createDirectories(dir);
            }

            id = statSystem.getSystemId(system);

            initTotal(system, statSystem);
            statRuntime = new StatRuntime(id);

            FiniteDuration d = duration(interval);
            intervalTask = system.scheduler().schedule(d, d,
                    this::saveRuntimeInterval, system.dispatcher());

            instance = this;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void initTotal(ActorSystem system, StatSystem statSystem) {
        totalRuntime = new StatRuntime(id);
        totalRuntime.availableProcessors = Runtime.getRuntime().availableProcessors();
        try {
            totalRuntime.path = system.provider().getDefaultAddress().toString();
        } catch (Exception ex) {
            statSystem.log("address error: " + ex);
        }
        saveTotal();
    }

    public static FiniteDuration duration(Duration duration) {
        long secs = duration.getSeconds();
        long nanos = duration.getNano();
        FiniteDuration d = FiniteDuration.apply(secs, TimeUnit.SECONDS);
        return d.plus(FiniteDuration.apply(nanos, TimeUnit.NANOSECONDS));
    }

    public synchronized void saveRuntimeInterval() {
        statRuntime.updateDuration();
        StatSystem statSystem = StatSystem.system();
        if (statSystem.getDir() != null) {
            try {
                Files.createDirectories(statSystem.getDir());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        statSystem.saveJsonp(vmDurationNameFormat, statRuntime);
        statRuntime = new StatRuntime(statRuntime.id);

        saveTotal();
    }

    public void saveTotal() {
        totalRuntime.updateDuration();
        Path file = StatSystem.system().getFile(vmTotalNameFormat, id);
        JsonWriter.write(totalRuntime.toJson(), file);
    }

    public static StatRuntime vmTotal(StatSystem statSystem, Instant[] timeRange, Stream<Path> vmFiles) {
        StatSystem.TotalCountReducer<StatRuntime> r = new StatSystem.TotalCountReducer<StatRuntime>() {
            @Override
            public StatRuntime createTotal() {
                return new StatRuntime(-1L);
            }

            @Override
            public StatRuntime create(Object json) {
                return new StatRuntime(json);
            }

            @Override
            public void update(StatRuntime r, StatRuntime next) {
                r.availableProcessors = Math.max(r.availableProcessors, next.availableProcessors);
                r.totalMemory = Math.max(r.totalMemory, next.totalMemory);
                r.freeMemory = Math.max(r.freeMemory, next.freeMemory);
                r.maxMemory = Math.max(r.maxMemory, next.maxMemory);
            }
        };
        return statSystem.total(timeRange, vmFiles, r);
    }

    public synchronized void addTimeoutNewActor() {
        totalRuntime.timeoutNewActor++;
        statRuntime.timeoutNewActor++;
    }

    public synchronized void addDeadLetter(DeadLetter l) {
        totalRuntime.addDeadLetter(l);
        statRuntime.addDeadLetter(l);
    }

    public synchronized void setNodes(List<String> nodes) {
        statRuntime.nodes = nodes;
        totalRuntime.nodes = nodes;
    }

    public synchronized void setRemainActorCount(long count) {
        statRuntime.remainActorCount = count;
        totalRuntime.remainActorCount = count;
    }

    public synchronized void addNewActorCount(long n) {
        statRuntime.newActorCount += n;
        totalRuntime.newActorCount += n;
    }

    public synchronized void addCreationTime(Duration d) {
        statRuntime.creationTime = statRuntime.creationTime.plus(d);
        totalRuntime.creationTime = totalRuntime.creationTime.plus(d);
    }

    public synchronized void addTimeoutNextManager() {
        totalRuntime.timeoutNextManager++;
        statRuntime.timeoutNextManager++;
    }

    public synchronized void addCreated(int forwardedCount, boolean success) {
        totalRuntime.addForwarded(forwardedCount, success);
        statRuntime.addForwarded(forwardedCount, success);
    }

    public static class StatRuntime extends StatSystem.StatBase {
        public long totalMemory;
        public long maxMemory;
        public long freeMemory;

        public String path = "";
        public long newActorCount;
        /** -1 means same value as previous*/
        public long remainActorCount = -1;
        /** changed nodes */
        public List<String> nodes = Collections.emptyList();

        public Duration creationTime = Duration.ZERO;

        /** optional: 0 */
        public int availableProcessors;

        public long timeoutNewActor;
        public long timeoutNextManager;

        public Map<String,Long> deadLetters = new HashMap<>();

        public Map<Integer,Long> forwardedSuccessCounts = new HashMap<>();
        public Map<Integer,Long> forwardedFailCounts = new HashMap<>();

        public StatRuntime(long id) {
            super(id);
        }
        public StatRuntime(Object json) {
            super(-1);
            setJson(json);
        }

        public void addDeadLetter(DeadLetter l) {
            String name = l.message().getClass().getName();
            deadLetters.compute(name, (k,v) -> v == null ? 1 : (1 + v));
        }

        public void addForwarded(int c, boolean success) {
            (success ? forwardedSuccessCounts: forwardedFailCounts)
                    .compute(c, (k,v) -> (v == null ? 1L : 1L + v));
        }

        @Override
        public void updateDuration() {
            super.updateDuration();
            Runtime rt = Runtime.getRuntime();
            totalMemory = rt.totalMemory();
            maxMemory = rt.maxMemory();
            freeMemory = rt.freeMemory();
        }

        @Override
        public Map<String, Object> toJson() {
            Map<String,Object> json = super.toJson();
            updateDuration();
            json.put("maxMemory", maxMemory);
            json.put("freeMemory", freeMemory);
            json.put("totalMemory", totalMemory);
            if (!path.isEmpty()) {
                json.put("path", path);
            }
            json.put("newActorCount", newActorCount);
            json.put("remainActorCount", remainActorCount);
            json.put("nodes", nodes);
            json.put("creationTime", creationTime.toString());
            if (availableProcessors != 0) {
                json.put("availableProcessors", availableProcessors);
            }
            json.put("timeoutNewActor", timeoutNewActor);
            json.put("deadLetters", new HashMap<>(deadLetters));
            json.put("timeoutNextManager", timeoutNextManager);

            Map<String,Object> fMap = new HashMap<>();
            forwardedSuccessCounts.forEach((k,v) -> fMap.put(Integer.toString(k), v));
            json.put("forwardedSuccessCounts", fMap);

            Map<String,Object> fFMap = new HashMap<>();
            forwardedFailCounts.forEach((k,v) -> fFMap.put(Integer.toString(k), v));
            json.put("forwardedFailCounts", fFMap);

            return json;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void setJson(Object json) {
            super.setJson(json);
            Map<String,Object> map = (Map<String,Object>) json;
            maxMemory = longValue(map, "maxMemory");
            freeMemory = longValue(map, "freeMemory");
            totalMemory = longValue(map, "totalMemory");
            path = (String) map.getOrDefault("path", "");
            newActorCount = longValue(map, "newActorCount");
            remainActorCount = longValue(map, "remainActorCount");
            nodes = (List<String>) map.getOrDefault("nodes", new ArrayList<>(0));
            creationTime = Duration.parse((String) map.getOrDefault("creationTime", "PT0S"));
            availableProcessors = ((Number) map.getOrDefault("availableProcessors", 0)).intValue();
            timeoutNewActor = longValue(map, "timeoutNewActor");
            deadLetters = (Map<String,Long>) map.getOrDefault("deadLetters", new HashMap<>());
            timeoutNextManager = longValue(map, "timeoutNextManager");

            forwardedSuccessCounts = new HashMap<>();
            Map<String,Number> fMap = (Map<String,Number>) map.getOrDefault("forwardedSuccessCounts", new HashMap<>());
            fMap.forEach((k,v) -> forwardedSuccessCounts.put(Integer.valueOf(k), v.longValue()));

            forwardedFailCounts = new HashMap<>();
            Map<String,Number> fFMap = (Map<String,Number>) map.getOrDefault("forwardedFailCounts", new HashMap<>());
            fFMap.forEach((k,v) -> forwardedSuccessCounts.put(Integer.valueOf(k), v.longValue()));
        }
    }

    public static class GraphImageSetter {
        protected ActorSystem system;
        protected StatSystem statSystem;
        protected Path dotFile;

        public static String imageNameFormat = "stimg%d.png";
        public static String imageNameVmFormat = "vmimg%d.png";

        public GraphImageSetter(ActorSystem system, Path dotFile) {
            this(system, dotFile, StatSystem.system());
        }

        public GraphImageSetter(ActorSystem system, Path dotFile, StatSystem statSystem) {
            this.system = system;
            this.statSystem = statSystem;
            this.dotFile = dotFile;
        }

        public void saveVms(DotGraph g) {
            if (statSystem.isEnabled()) {
                List<Path> vms = statSystem.findFiles(StatVm.vmDurationNamePattern)
                        .collect(Collectors.toList());

                vms.forEach(f -> saveVm(g, f));
            }
        }

        public void saveVm(DotGraph g, Path vmFile) {
            long vid = getVmId(vmFile);
            String label = getVmLabel(vid);
            DotNode vmn = g.add(new DotNode(label));
            vmn.getAttrs().add(DotAttribute.shapeBox());
            setNodeImage(vmn, vid, true);
        }

        public long getVmId(Path vmFile) {
            long vid;
            Matcher m = StatVm.vmDurationNamePattern.matcher(vmFile.getFileName().toString());
            if (m.matches()) {
                vid = Long.valueOf(m.group(1));
            } else {
                System.err.println("vmfile error: " + vmFile.getFileName().toString());
                vid = statSystem.getSystemId(system);
            }
            return vid;
        }

        public String getVmLabel(long vid) {
            String label = "vm: id=" + vid;
            Path totalPath = statSystem.getFile(StatVm.vmTotalNameFormat, vid);
            if (Files.exists(totalPath)) {
                try {
                    if (Files.size(totalPath) > 0) {
                        StatVm.StatRuntime total = new StatVm.StatRuntime(JsonReader.read(totalPath.toFile()));
                        if (!total.path.isEmpty()) {
                            label += ",path=" + total.path;
                        }
                        label += ",maxMem=" + String.format("%,d", total.maxMemory);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            return label;
        }

        public void setNodeImage(DotNode vmn, long vid, boolean vmNode) {
            String path = getImagePath(vmNode ? imageNameVmFormat : imageNameFormat, vid);
            if (path != null) {
                vmn.getAttrs().addQuote("image", path);
            }
        }

        public static Path parent(Path p) {
            return p == null ? Paths.get(".") :
                    (p.getParent() == null ? Paths.get(".") :
                            p.getParent());
        }

        public String getImagePath(String format, long id) {
            if (dotFile == null || !statSystem.isEnabled()) {
                return null;
            }
            Path imgFile = statSystem.getFile(format, id);
            if (!Files.exists(parent(imgFile))) {
                return null;
            }
            if (!Files.exists(imgFile)) {
                saveEmptyImage(imgFile);
            }
            try {
                return parent(dotFile).relativize(imgFile).toString();
            } catch (Exception ex) {
                System.err.println(dotFile + " : " + imgFile);
                throw new RuntimeException(ex);
            }
        }

        public void saveEmptyImage(Path imgFile) {
            try {
                BufferedImage img = new BufferedImage(10, 10, BufferedImage.TYPE_3BYTE_BGR);
                Graphics2D g = img.createGraphics();
                g.setPaint(Color.blue);
                g.fillRect(0, 0, 10, 10);

                ImageIO.write(img, "png", imgFile.toFile());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
}
