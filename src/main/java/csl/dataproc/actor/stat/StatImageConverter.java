package csl.dataproc.actor.stat;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * replace image files initially generated by {@link csl.dataproc.actor.vfdt.ActorVfdtVisitorDot}.
 *   <pre>
 *       tree-stats/.../stimg*.png
 *   </pre>
 *   Those images are loaded from tree.dot.
 *
 *   load tree-stats/.../rd*.jsonp.
 *
 *   read "countToClass" : {  "Cls" : {"consumed":..., "count":countInt}, ... } map
 *     and map to list of {entry.key.hashCode, entry.value.count / total.count} and sort by the ratio.
 *     It determines a color-hue from the hashCode and height of the bar region from the ratio.
 *     The width is determined from duration of the jsonp line.
 *     Also, a color brightness is determined by consumed/duration;
 *      a higher idle time becomes a darker color.
 */
public class StatImageConverter {
    public static void main(String[] args) {
        new StatImageConverter().run(args);
    }

    protected StatReceive.ReceiveCount entireReceive;
    protected StatMailbox.MailboxInOutCount entireMailbox;
    protected StatVm.StatRuntime entireVm;

    protected Instant[] timeRange;
    protected int width = 200;
    protected int height = 80;
    protected Path outDir;

    protected StatSystem statSystem = StatSystem.system();

    public void run(String... args) {
        runDir(Paths.get(args[0]));
    }

    public void runDir(Path dir) {
        try {
            statSystem.setDir(dir);
            outDir = dir;

            List<Path> receiveFiles = statSystem.findFiles(StatReceive.durationNamePattern)
                    .collect(Collectors.toList());
            List<Path> mailboxFiles = statSystem.findFiles(StatMailbox.durationNamePattern)
                    .collect(Collectors.toList());

            List<Path> vmFiles = statSystem.findFiles(StatVm.vmDurationNamePattern)
                    .collect(Collectors.toList());

            log("files: " + (receiveFiles.size() + mailboxFiles.size() + vmFiles.size()));

            timeRange = statSystem.timeRangeJsonp(Stream.concat(
                    receiveFiles.stream(),
                    mailboxFiles.stream()));

            entireReceive = StatReceive.receiveTotal(statSystem, timeRange, receiveFiles.stream());
            entireMailbox = StatMailbox.mailboxTotal(statSystem, timeRange, mailboxFiles.stream());
            entireVm = StatVm.vmTotal(statSystem, timeRange, vmFiles.stream());

            log("range: " + entireReceive.toJson());

            Set<Long> ids = Stream.concat(
                    idFromFiles(receiveFiles, StatReceive.durationNamePattern),
                    idFromFiles(mailboxFiles, StatMailbox.durationNamePattern))
                    .collect(Collectors.toSet());

            ids.forEach(this::save);

            idFromFiles(vmFiles, StatVm.vmDurationNamePattern)
                    .forEach(this::saveVm);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    public void log(String s) {
        System.err.println(s);
    }


    public Stream<Long> idFromFiles(List<Path> files, Pattern pattern) {
        return files.stream()
                .map(p -> pattern.matcher(p.getFileName().toString()))
                .filter(Matcher::matches)
                .map(m -> m.group(1))
                .map(Long::valueOf);
    }

    public void save(long id) {
        BufferedImage img = createImage(width, height);
        Graphics2D g = img.createGraphics();
        double eachWidth = width;
        double eachHeight = height / 2.0;

        double nextX = 0;
        double nextY = 0;

        renderReceive(id, g, nextX, nextY, eachWidth, eachHeight);
        nextY += eachHeight;
        renderMailbox(id, g, nextX, nextY, eachWidth, eachHeight);

        g.dispose();

        saveImage(img, StatVm.GraphImageSetter.imageNameFormat, id);
    }

    protected void saveImage(BufferedImage img, String format, long id) {
        Path outFile = statSystem.getFile(format, id);
        try {
            if (Files.exists(outFile)) {
                log("overwrite " + outFile);
            } else {
                log("write " + outFile);
            }
            ImageIO.write(img, "png", outFile.toFile());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void renderReceive(long id, Graphics2D g, double nextX, double nextY, double eachWidth, double eachHeight) {
        Path receiveFile = statSystem.getFile(StatReceive.durationNameFormat, id);
        g.setColor(Color.gray);
        g.drawString("receive", (int) nextX + 10, (int) nextY + 10);
        if (Files.isRegularFile(receiveFile)) {
            try (BufferedReader reader = Files.newBufferedReader(receiveFile)) {
                Stream<StatReceive.ReceiveCount> list = statSystem.parseJsonp(reader, StatReceive.ReceiveCount::new);
                list.forEach(e -> renderReceive(eachWidth, eachHeight, e, entireReceive, imageRenderer(g, nextX, nextY)));
            } catch (Exception ex) {
                throw new RuntimeException(receiveFile.toString(), ex);
            }
        }
    }

    public void renderMailbox(long id, Graphics2D g, double nextX, double nextY, double eachWidth, double eachHeight) {
        Path mailboxFile = statSystem.getFile(StatMailbox.durationNameFormat, id);
        g.setColor(Color.gray);
        g.drawString("mailbox", (int) nextX + 10, (int) nextY + 10);
        if (Files.isRegularFile(mailboxFile)) {
            try (BufferedReader reader = Files.newBufferedReader(mailboxFile)) {
                Stream<StatMailbox.MailboxInOutCount> list = statSystem.parseJsonp(reader, StatMailbox.MailboxInOutCount::new);
                list.forEach(i -> renderMailbox(eachWidth, eachHeight, i, entireMailbox, imageRenderer(g, nextX, nextY)));
            } catch (Exception ex) {
                throw new RuntimeException(mailboxFile.toString(), ex);
            }
        }
    }

    public void saveVm(long id) {
        BufferedImage img = createImage(width, height / 2);

        Graphics2D g = img.createGraphics();
        Path vmFile = statSystem.getFile(StatVm.vmDurationNameFormat, id);
        g.setColor(Color.gray);
        g.drawString("memory", 10, 10);

        if (Files.isRegularFile(vmFile)) {
            try (BufferedReader reader = Files.newBufferedReader(vmFile)) {
                Stream<StatVm.StatRuntime> list = statSystem.parseJsonp(reader, StatVm.StatRuntime::new);
                list.forEach(i -> renderVm(img.getWidth(), img.getHeight(), i, entireVm, imageRenderer(g, 0, 0)));
            } catch (Exception ex) {
                throw new RuntimeException(vmFile.toString(), ex);
            }
        }

        g.dispose();
        saveImage(img, StatVm.GraphImageSetter.imageNameVmFormat, id);
    }

    protected BufferedImage createImage(int width, int height) {
        BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_4BYTE_ABGR);
        Graphics2D g = img.createGraphics();

        Color backColor = new Color(0.8f, 0.8f, 0.8f, 0.3f);

        g.setPaint(backColor);
        g.fillRect(0, 0, width, height);
        return img;
    }

    ////////////////


    public interface StatRenderer<StatType> {
        /** posXY: {x,y,w,h,hue,brightness} */
        void apply(double[] posXY, StatType count, StatType range);
    }

    public <StatType> StatRenderer<StatType> imageRenderer(Graphics2D g, double nextX, double nextY) {
        return (posXY, count, range) -> {
            Rectangle2D r = new Rectangle2D.Double(nextX + posXY[0], nextY + posXY[1], posXY[2], posXY[3]);
            g.setPaint(Color.getHSBColor((float) posXY[4], 0.6f, (float) posXY[5]));
            g.fill(r);
        };
    }

    protected Map<Object, Integer> valueToIndex = new HashMap<>();

    public void renderReceive(double w, double h, StatReceive.ReceiveCount count, StatReceive.ReceiveCount range,
                              StatRenderer<StatReceive.ReceiveCount> r) {
        double timeX1 = timePosition(Duration.between(range.start, count.start), range.duration) * w;
        double timeW = Math.max(timePosition(count.duration, range.duration) * w, 1);

        double countH = (count.count / (double) range.count) * h;
        double countY1 = Math.max(h - countH, 1);

        double consumedBr = (0.3 - timePosition(count.consumed, count.duration) * 0.3) + 0.5;

        int stateNum = valueToIndex(count.state);

        for (double[] e : count.classToCount.entrySet().stream()
                .map(e -> new double[] {
                        valueToIndex(e.getKey()),
                        e.getValue().count / (double) count.count}) //{idx, ratio}
                .sorted(Comparator.comparingDouble(e -> e[1]))
                .collect(Collectors.toList())) {
            int idx = (int) e[0];
            double ratio = e[1];
            int colorId = colorId(stateNum, idx);

            r.apply(new double[] {timeX1, countY1, timeW, ratio * countH,
                nextHue(colorId),
                consumedBr}, count, range);
            countY1 += ratio * countH;
        }
    }

    ////////////////


    public void renderMailbox(double w, double h, StatMailbox.MailboxInOutCount count, StatMailbox.MailboxInOutCount range,
                              StatRenderer<StatMailbox.MailboxInOutCount> r) {
        double timeX1 = timePosition(Duration.between(range.start, count.start), range.duration) * w;
        double timeW = Math.max(timePosition(count.duration, range.duration) * w, 1);

        double countH = (count.maxQueueSize() / (double) range.maxQueueSize()) * h;
        double countY1 = Math.max(h - countH, 1);

        double consumedBr = (0.3 - (count.queueCount() / (double) range.queueCount()) * 0.3) + 0.5;

        for (double[] e : count.classToCount.entrySet().stream()
                .map(e -> new double[] {
                        valueToIndex(e.getKey()),
                        e.getValue().count / (double) count.totalEnqueueCount.count
                }).collect(Collectors.toList())) {
            int idx = (int) e[0];
            double ratio = e[1];
            int colorId = colorId(1, idx);

            r.apply(new double[] {timeX1, countY1, timeW, ratio * countH,
                    nextHue(colorId),
                    consumedBr}, count, range);
            countY1 += ratio * countH;
        }
    }


    ////////////////

    protected int valueToIndex(Object hash) {
        return valueToIndex.computeIfAbsent(hash, k -> valueToIndex.size());
    }

    protected int colorId(int stateNum, int count) {
        return ((count % 16) << 4) | (stateNum % 16);
    }

    public Double nextHue(int n) {
        int max = 128;
        return (n % 2 == 0 ? ((n + max / 2) % max) : n) / (double) max;
    }

    private double timePosition(Duration time, Duration range) {
            return (time.getSeconds() + (time.getNano() / 1000_000_000.0)) /
                    ((double) range.getSeconds() + (range.getNano() / 1000_000_000.0));
    }

    ////////////////

    public void renderVm(double w, double h, StatVm.StatRuntime count, StatVm.StatRuntime range, StatRenderer<StatVm.StatRuntime> r) {
        double timeX1 = timePosition(Duration.between(range.start, count.start), range.duration) * w;
        double timeW = Math.max(timePosition(count.duration, range.duration) * w, 1);

        long vmUsage = range.totalMemory + range.freeMemory;
        long max = Math.min(range.maxMemory, vmUsage * 2L);

        long countUsage = count.totalMemory + count.freeMemory;

        double countH = (countUsage / (double) max) * h;
        double countY1 = Math.max(h - countH, 1);

        {
            int idx = 3;
            double ratio = count.freeMemory / (double) countUsage;
            int colorId = colorId(1, idx);

            r.apply(new double[] {timeX1, countY1, timeW, ratio * countH,
                    nextHue(colorId),
                    0.8f}, count, range);
            countY1 += ratio * countH;
        }

        {
            int idx = 1;
            double ratio = count.totalMemory / (double) countUsage;
            int colorId = colorId(1, idx);

            r.apply(new double[] {timeX1, countY1, timeW, ratio * countH,
                    nextHue(colorId),
                    0.8f}, count, range);
            countY1 += ratio * countH;
        }

    }
}
