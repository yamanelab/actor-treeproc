package csl.dataproc.actor.birch;

import csl.dataproc.dot.DotAttribute;
import csl.dataproc.dot.DotEdge;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.dot.DotNode;
import csl.dataproc.tgls.util.JsonReader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * <pre>
 *     BirchDotSteps trace.jsonp outdir
 * </pre>
 * The program load the trace.jsonp file and visualize the steps of the file as step-NNNNN.dot files.
 * The trace file is generated by {@link ActorBirchBatch.PutTraceJson}.
 *
 * <pre>
 *
 *     $ mkdir imgs
 *     $ cd outdir
 *     $ for f in $(ls "*.dot") ; do dot -Tpng $f -o ../imgs/$f.png ; done
 * </pre>
 */
public class BirchDotSteps {
    public static void main(String[] args) {
        new BirchDotSteps().run(Paths.get(args[0]), Paths.get(args[1]));
    }

    public void run(Path file, Path outDir) {
        try {
            this.outDir = outDir;
            start();
            Files.createDirectories(outDir);
            Files.readAllLines(file).stream()
                    .map(this::parseJson)
                    .forEach(this::runLine);
            end();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    protected Path outDir;
    protected DotEnv env = new DotEnv();
    protected int lineCount;

    DotNode stepNode;
    String currentStepName;

    public Object parseJson(String line) {
        try {
            return JsonReader.v(line).parseObject();
        } catch (Exception ex) {
            System.err.println("error: " + lineCount + " : " + line);
            ex.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public void runLine(Object json) {
        if (json != null && json instanceof Map) {
            Map<String,Object> map = (Map<String,Object>) json;
            String type = getMapString(map, "type");
            currentStepName = type;
            switch (type) {
                case ActorBirchBatch.PutTraceJson.newRootChangeActor:
                    runNewRootChangeActor(map);
                    break;
                case ActorBirchBatch.PutTraceJson.changeRoot:
                    runChangeRoot(map);
                    break;
                case ActorBirchBatch.PutTraceJson.putToRoot:
                    runPutToRoot(map);
                    break;
                case ActorBirchBatch.PutTraceJson.putStart:
                    runPutStart(map);
                    break;
                case ActorBirchBatch.PutTraceJson.putEntry:
                    runPutEntry(map);
                    break;
                case ActorBirchBatch.PutTraceJson.getChildrenCfs:
                    runGetChildrenCfs(map);
                    break;
                case ActorBirchBatch.PutTraceJson.putWithChildrenCfs:
                    runPutWithChildrenCfs(map);
                    break;
                case ActorBirchBatch.PutTraceJson.findClosestChild:
                    runFindClosestChild(map);
                    break;
                case ActorBirchBatch.PutTraceJson.finishWithAddEntry:
                    runFinishWithAddEntry(map);
                    break;
                case ActorBirchBatch.PutTraceJson.splitByNewEntry:
                    runSplitByNewEntry(map);
                    break;
                case ActorBirchBatch.PutTraceJson.addNewNode:
                    runAddNewNode(map);
                    break;
                case ActorBirchBatch.PutTraceJson.addNewNodeEntry:
                    runAddNewNodeEntry(map);
                    break;
                case ActorBirchBatch.PutTraceJson.splitByNewNode:
                    runSplitByNewNode(map);
                    break;
                case ActorBirchBatch.PutTraceJson.split:
                    runSplit(map);
                    break;
                case ActorBirchBatch.PutTraceJson.newRoot:
                    runNewRoot(map);
                    break;
                case ActorBirchBatch.PutTraceJson.finishWithAddNodeTree:
                    runFinisWithAddNodeTree(map);
                    break;
                case ActorBirchBatch.PutTraceJson.finishWithAddNodeEntry:
                    runFinishWithAddNodeEntry(map);
                    break;
                case ActorBirchBatch.PutTraceJson.addNewNodeToLink:
                    runAddNewNodeToLink(map);
                    break;
            }
        }
        lineCount++;
    }

    public void end() {
        setStep("FINISH", null);
        new ArrayList<>(env.getEdgeGroup().keySet())
                .forEach(env::removeEdgeGroup);
        currentStepName = "finish";
        save();

        Path nodeMap = outDir.resolve("node-map.txt");
        List<String> lines = new ArrayList<>();
        env.getIdToNode().forEach((k,v) -> {
            List<String> aliasNames = env.getAliasMap().entrySet().stream()
                    .filter(e -> e.getValue().equals(k))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            lines.add(k + ":");
            aliasNames.forEach(a -> lines.add("\t" + a));
        });
        try {
            Files.write(nodeMap, lines);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static String getMapString(Map<String,Object> map, String key) {
        Object v= map.getOrDefault(key, "");
        if (v instanceof String) {
            return (String) v;
        } else {
            return "";
        }
    }

    public void save() {

        setDepth();

        Path nextPath = outDir.resolve(String.format("step-%05d-%s.dot", lineCount, currentStepName));
        env.getGraph().save(nextPath.toFile());

    }

    public void setDepth() {
        DotNode root = env.getGraph().getEdges().stream()
                .filter(e -> env.getEdgeLabel(e).equals("root"))
                .map(DotEdge::getTo)
                .findFirst()
                .orElse(null);
        if (root != null) {
            setDepth(root, new ArrayList<>(), 0 );
        }
    }

    private void setDepth(DotNode n, List<DotNode> path, int level) {
        if (path.contains(n)) {
            return;
        }
        List<DotNode> next = new ArrayList<>(path);
        next.add(n);
        for (DotEdge e : env.getEdgesFrom(n, "child")) {
            e.getAttrs().get("label").value = DotAttribute.quote("child:" + level);
            setDepth(e.getTo(), next, level + 1);
        }
    }

    public void start() {
        env.getGraph().getGraphAttrs().add(DotAttribute.graphRankDirectionLR());
        stepNode = env.getGraph().add("step!");
        stepNode.getAttrs().add(DotAttribute.shapeBox());
        lineCount = 0;

    }

    public void setStep(String label, String inputPath) {
        stepNode.getAttrs().addQuote("label", lineCount + ":" + label);
        setInputColor(stepNode, inputPath);
    }

    public void setThisForInputPath(String inputPath, DotNode thisNode) {
        env.getIdToNode().put(inputPath + "@this", thisNode);
    }

    public DotNode getThisForInputPath(String inputPath) {
        DotNode n = env.getIdToNode().get(inputPath + "@this");
        if (n == null) {
            n = env.getIdToNode().get(inputPath);
        }
        return n;
    }


    public void runNewRootChangeActor(Map<String, Object> map) {
        setStep("newRootChangeActor", null);
        DotNode node = env.addPathNode(map, "rootChanger");
        node.getAttrs().addQuote("label", "rootChanger");
        save();
    }

    public void runChangeRoot(Map<String,Object> map) {
        setStep("changeRoot", getMapString(map, "input"));
        DotNode rootChanger = env.addPathNode(map, "rootChanger");
        DotNode root = env.addPathNode(map, "root");
        env.replaceEdge(rootChanger, root, "root");
        save();
    }

    public void runPutToRoot(Map<String,Object> map) {
        /*
        DotNode rootChanger = env.addPathNode(map, "rootChanger");
        DotNode input = env.addPathNode(map, "input");
        env.getGraph().addEdge(input, rootChanger).getAttrs().addQuote("label", "putToRoot");
        save();
        */
    }

    public void runPutStart(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".putStart", inputPath);
        DotNode input = env.addPathNode(map, "input");
        setThisForInputPath(inputPath, node);

        setInputColor(input, inputPath);
        setInputColor(env.addEdge(input, node,"putStart:" + inputPath, inputPath), inputPath);
        save();
    }

    public void setInputColor(DotNode node, String inputPath) {
        node.getAttrs().getList()
                .removeIf(e -> e.key.equals("color") || e.key.equals("fontcolor"));

        DotAttribute a = getInputColor(inputPath);
        if (a != null) {
            node.getAttrs().add(a);
            node.getAttrs().add("fontcolor", a.value);
        }
    }

    public void setInputColor(DotEdge edge, String inputPath) {
        edge.getAttrs().getList()
                .removeIf(e -> e.key.equals("color") || e.key.equals("fontcolor"));
        if (inputPath != null) {
            DotAttribute a = getInputColor(inputPath);
            if (a != null) {
                edge.getAttrs().add(a);
                edge.getAttrs().add("fontcolor", a.value);
            }
        }
    }

    public void runPutEntry(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".putEntry", inputPath);
        DotNode input = env.addPathNode(map, "input");
        setInputColor(input, inputPath);
        DotNode from = getThisForInputPath(inputPath);
        setThisForInputPath(inputPath, node);

        if (from == null) {
            from = input;
        }
        setInputColor(env.addEdge(from, node, "putEntry:" + inputPath, inputPath), inputPath);
        save();
    }

    public void runGetChildrenCfs(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".getChildrenCfs", inputPath);

        setThisForInputPath(inputPath, node);

        DotNode parent = env.addPathNode(map, "parent");
        setInputColor(env.addEdge(parent, node,
                "getChildrenCfs:" + map.get("index") + "," + map.get("postProcess") + "," + inputPath,
                    inputPath), inputPath);
        save();
    }

    @SuppressWarnings("unchecked")
    public void runPutWithChildrenCfs(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".putWithChildrenCfs", inputPath);

        DotNode from = getThisForInputPath(inputPath);
        setThisForInputPath(inputPath, node);

        List<Map<String,Object>> nodes = (List<Map<String,Object>>) map.get("collected");
        setInputColor(env.addEdge(from, node, "putWithChildrenCfs:[" + nodes.size() + "]" + "," + inputPath, inputPath), inputPath);

        for (Map<String,Object> n : nodes) {
            DotNode subNode = env.addPathNode(getMapString(n, "node"));
            setInputColor(env.addEdge(node, subNode, "collected:" + n.get("index") + ",cf=" + n.get("cf") + "," + inputPath, inputPath), inputPath);
        }
        save();
    }

    public void runFindClosestChild(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".findClosestChild", inputPath);

        DotNode from = getThisForInputPath(inputPath);
        setThisForInputPath(inputPath, node);

        setInputColor(env.addEdge(from, node,
                "findClosestChild:" + map.get("childIndex") + "," + map.get("selectedD") + "," + inputPath,
                        inputPath), inputPath);
        save();
    }

    public void runSplitByNewEntry(Map<String,Object> map) {

        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".splitByNewEntry", inputPath);

        DotNode from = getThisForInputPath(inputPath);
        setThisForInputPath(inputPath, node);

        setInputColor(env.addEdge(from, node, "splitByNewEntry:" + inputPath, inputPath), inputPath);
        save();
    }

    public void runAddNewNode(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".addNewNode", inputPath);


        DotNode from = getThisForInputPath(inputPath);
        setThisForInputPath(inputPath, node);
        DotNode newNode = env.addPathNode(getMapString(map, "newNode"));
        setInputColor(newNode, inputPath);

        setInputColor(env.addEdge(from, node, "addNewNode", inputPath), inputPath);
        //it might not add the child to the node: env.addEdge(node, newNode, "child", null); //permanent
        save();
    }

    public void runAddNewNodeToLink(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".addNewNodeToLink", inputPath);

        int count = ((Number) map.get("count")).intValue();
        DotNode from = getThisForInputPath(inputPath);
        setThisForInputPath(inputPath, node);

        setInputColor(env.addEdge(from, node, "addNewNodeToLink:" + count, inputPath), inputPath);
        save();
    }

    @SuppressWarnings("unchecked")
    public void runAddNewNodeEntry(Map<String,Object> map) {

        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".addNewNodeEntry", inputPath);

        DotNode from = getThisForInputPath(inputPath);
        setThisForInputPath(inputPath, node);
        List<Map<String,Object>> nodes = (List<Map<String,Object>>) map.get("collected");

        String label = "addNewNodeEntry";
        if (nodes != null) {
            label += ":[" + nodes.size() + "]";
        }
        setInputColor(env.addEdge(from, node, label, inputPath), inputPath);

        save();
    }

    public void runSplitByNewNode(Map<String,Object> map) {

        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".splitByNewNode", inputPath);

        DotNode from = getThisForInputPath(inputPath);
        setThisForInputPath(inputPath, node);

        DotNode newNode = env.addPathNode(map, "newNode");
        setInputColor(newNode, inputPath);

        setInputColor(env.addEdge(from, node, "splitByNewNode", inputPath), inputPath);
        save();
    }

    @SuppressWarnings("unchecked")
    public void runSplit(Map<String,Object> map) {

        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".split", inputPath);

        DotNode newNode = env.addPathNode(map, "newNode");
        setInputColor(newNode, inputPath);

        env.getGraph().getEdges().removeAll(env.getEdgesFrom(node, "child"));

        List<String> lefts = (List<String>) map.get("left");
        List<String> rights = (List<String>) map.get("right");

        for (String l : lefts) {
            setInputColor(env.addEdge(node, env.addPathNode(l), "left", inputPath), inputPath);
            env.addEdge(node, env.addPathNode(l), "child", null);
        }

        for (String l : rights) {
            setInputColor(env.addEdge(node, env.addPathNode(l), "right", inputPath), inputPath);
            env.addEdge(newNode, env.addPathNode(l), "child", null);
        }

        save();
    }

    public void runNewRoot(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");

        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".newRoot", inputPath);


        DotNode newRoot = env.addPathNode(map, "newRoot");
        setInputColor(newRoot, inputPath);

        DotNode l = env.addPathNode(map, "left");
        DotNode r = env.addPathNode(map, "right");

        env.addEdge(newRoot, l, "child", null);
        env.addEdge(newRoot, r, "child", null);

        save();
        runFinish(map);
    }


    public void runFinishWithAddEntry(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".finishWithAddEntry", inputPath);
        DotNode nodeEntry = env.addPathNode(map, "node");

        setInputColor(env.addEdge(node, nodeEntry, "finishWithAddEntry:" + inputPath, inputPath), inputPath);

        env.addEdge(node, nodeEntry, "child", null);
        save();
        runFinish(map);
    }

    public void runFinishWithAddNodeEntry(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");
        String inputPath = getMapString(map, "input");
        String thisId = env.getPath(getMapString(map, "this"));
        setStep(thisId + ".finishWithAddNodeEntry", inputPath);

        DotNode newEntry = env.addPathNode(map, "target");
        setInputColor(newEntry, inputPath);

        env.addEdge(node, newEntry, "child", null);
        save();
        runFinish(map);
    }


    public void runFinisWithAddNodeTree(Map<String,Object> map) {
        DotNode node = env.addPathNode(map, "this");

        String thisId = env.getPath(getMapString(map, "this"));
        String inputPath = getMapString(map, "input");
        setStep(thisId + ".finishWithAddNodeTree", inputPath);

        DotNode subNode = env.addPathNode(map, "treeNode");

        env.addEdge(node, subNode, "child", null);

        save();
        runFinish(map);
    }



    public void runFinish(Map<String,Object> map) {
        String inputPath = getMapString(map, "input");
        env.removeNode(inputPath);
        env.removeEdgeGroup(inputPath);
        removeInputColor(inputPath);
    }

    List<InputColor> inputColors = new ArrayList<>();

    {
        int[] cols = {10, 50, 100, 150, 200};

        for (int r : cols) {
            for (int g : cols) {
                for (int b : cols) {
                    if (r == g && g == b) {
                        continue;
                    }
                    InputColor c = new InputColor();
                    c.attribute = DotAttribute.colorRGB(r, g, b);
                    inputColors.add(c);
                }
            }
        }
        Collections.shuffle(inputColors);
    }

    public void removeInputColor(String inputPath) {
        Set<String> pathColors = new HashSet<>();
        for (InputColor c : inputColors) {
            if (c.match(inputPath)) {
                c.inputPaths.remove(inputPath);
                pathColors.add(c.attribute.value);
            }
        }
        env.getGraph().getNodes()
                .forEach(n -> n.getAttrs().getList()
                        .removeIf(a -> (a.key.equals("color") || a.key.equals("fontcolor")) &&
                                        pathColors.contains(a.value)));
    }

    public DotAttribute getInputColor(String inputPath) {
        Optional<InputColor> c = inputColors.stream()
                .filter(e -> e.match(inputPath))
                .findFirst();
        if (c.isPresent()) { //existing path attr
            return c.get().attribute;
        }  else {
            inputColors.sort(Comparator.comparingInt(e -> e.inputPaths.size()));
            InputColor col = inputColors.get(0);
            col.inputPaths.add(inputPath);
            return col.attribute;
        }
    }

    public static class InputColor {
        public List<String> inputPaths = new ArrayList<>();
        public DotAttribute attribute;
        public boolean match(String path) {
            return inputPaths.contains(path);
        }
    }

    public interface Command {
        void run(DotEnv env);
    }

    public static class CommandParser {
        public Command parse(String line) {
            return null;
        }
    }

    public static class DotEnv {
        protected DotGraph graph = new DotGraph();
        protected Map<String,DotNode> idToNode = new LinkedHashMap<>();
        protected Map<String,String> aliasMap = new LinkedHashMap<>();

        protected Map<String,List<DotEdge>> edgeGroup = new HashMap<>();

        public DotGraph getGraph() {
            return graph;
        }

        public Map<String, DotNode> getIdToNode() {
            return idToNode;
        }

        public Map<String, String> getAliasMap() {
            return aliasMap;
        }

        public Map<String, List<DotEdge>> getEdgeGroup() {
            return edgeGroup;
        }

        public List<DotEdge> getEdgeGroup(String key) {
            return edgeGroup.computeIfAbsent(key, k -> new ArrayList<>());
        }

        /**
         * @param path  a path indicating a node
         * @return key for the path
         */
        public String getPath(String path) {
            if (aliasMap.containsKey(path)) { //if a result path is already registered, returns it
                return aliasMap.get(path);
            } else if (path.startsWith("ref:")) { //actor reference by ActorBirchRefNode or simply ActorRef, with an index number
                String n = "ref:[" + aliasMap.size() + "]";
                aliasMap.put(path, n);
                return n;
            } else if (path.startsWith("node:IndexedEntry@")) { //IndexedEntry node
                String id = "E:" + path.substring("node:IndexedEntry@".length());
                aliasMap.put(path, id);
                return id;
            } else {
                return path;
            }
        }

        public DotNode addPathNode(String path) {
            path = getPath(path);
            return idToNode.computeIfAbsent(path, graph::add); //add an node with the path as the label
        }

        /** get path by the key from the map, and obtains the node for the path.
         *   also, the node will be registered with the key. */
        public DotNode addPathNode(Map<String,Object> map, String key) {
            DotNode node = addPathNode(getMapString(map, key));

            idToNode.put(key, node);
            return node;
        }

        public void removeNode(String key) {
            removeNode(idToNode.get(key));
        }

        public void removeNode(DotNode n) {
            if (n != null) {
                graph.getNodes().remove(n);
                graph.getEdges().removeIf(e -> e.getFrom().equals(n) || e.getTo().equals(n));
            }
        }

        /** add edge between from and to nodes, with the label name, and register the edge as the group if provided
         *  <pre>
         *      [from] - - name - - &gt; [to]
         *  </pre>
         * */
        public DotEdge addEdge(DotNode from, DotNode to, String name, String group) {
            DotEdge e = graph.addEdge(from, to);
            e.getAttrs().addQuote("label", name);
            if (group != null) {
                getEdgeGroup(group).add(e);
                e.getAttrs().add(DotAttribute.styleDotted());
            }
            return e;
        }

        /** remove any existing nodes that contain the name, and add an edge with the name
         * <pre>
         *     [from] ---name--&gt; [to]
         * </pre>
         * */
        public DotEdge replaceEdge(DotNode from, DotNode to, String name) {
            removeEdge(name);
            DotEdge e = graph.addEdge(from, to);
            e.getAttrs().addQuote("label", name);
            return e;
        }

        /** remove edges containing the label within the label string "str:..." before of ":".
         *  e.g. "hell" matches to "hello:world" */
        public void removeEdge(String label) {
            List<DotEdge> es = graph.getEdges().stream()
                    .filter(e -> getEdgeLabel(e).contains(label))
                    .collect(Collectors.toList());

            graph.getEdges().removeAll(es);
        }

        public void removeEdges(String... labels) {
            removeEdges(Arrays.asList(labels));
        }
        public void removeEdges(Collection<String> labels) {
            List<DotEdge> es = graph.getEdges().stream()
                    .filter(e -> {
                        String l = getEdgeLabel(e);
                        return labels.stream()
                                .anyMatch(lb -> l.contains(l));
                    })
                    .collect(Collectors.toList());
            graph.getEdges().removeAll(es);
        }

        public String getEdgeLabel(DotEdge e) {
            DotAttribute a = e.getAttrs().get("label");
            if (a != null) {
                String l = DotAttribute.unquote(a.value);
                int i = l.indexOf(":");
                if (i != -1) {
                    return l.substring(0, i);
                } else {
                    return l;
                }
            } else {
                return "";
            }
        }

        public void removeEdgeGroup(String k) {
            List<DotEdge> es = edgeGroup.remove(k);
            if (es != null) {
                graph.getEdges().removeAll(es);
            }
        }

        public List<DotEdge> getEdgesFrom(DotNode from, String label) {
            return graph.getEdges().stream()
                    .filter(e -> e.getFrom().equals(from))
                    .filter(e -> label == null || getEdgeLabel(e).contains(label))
                    .collect(Collectors.toList());
        }

        public List<DotEdge> getEdgesTo(DotNode to, String label) {
            return graph.getEdges().stream()
                    .filter(e -> e.getTo().equals(to))
                    .filter(e -> label == null || getEdgeLabel(e).contains(label))
                    .collect(Collectors.toList());
        }
    }
}
