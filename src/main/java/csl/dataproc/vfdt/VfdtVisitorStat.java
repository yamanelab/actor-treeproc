package csl.dataproc.vfdt;

import csl.dataproc.csv.CsvWriter;
import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.tgls.util.ArraysAsList;
import csl.dataproc.tgls.util.JsonWriter;
import csl.dataproc.vfdt.count.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VfdtVisitorStat extends VfdtNode.NodeVisitor {
    protected LinkedList<String> dirStack = new LinkedList<>();
    protected long totalCount = 0;

    public void run(Path dir, VfdtNode node) {
        dirStack.push(dir.toString());
        createDirectories(dir);
        node.accept(this);
    }

    protected void createDirectories(Path p) {
        try {
            Files.createDirectories(p);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean visitSplit(VfdtNodeSplit node) {
        dirStack.push(Paths.get(dirStack.peek()).resolve("-1").toString());
        return true;
    }

    @Override
    public void visitSplitChild(VfdtNodeSplit node, int index, VfdtNode child) {
        dirStack.pop();
        Path p = Paths.get(dirStack.peek()).resolve(String.valueOf(index));
        dirStack.push(p.toString());
        createDirectories(p);
    }

    @Override
    public void visitAfterSplit(VfdtNodeSplit node) {
        dirStack.pop();
    }

    protected void writeJson(Object json, String fileName) {
        JsonWriter.write(json, Paths.get(dirStack.peek()).resolve(fileName));
    }

    @Override
    public boolean visitSplitContinuous(VfdtNodeSplitContinuous node) {
        writeJson(toJsonSplitContinuous(node), "node-info.json");
        return visitSplit(node);
    }

    public LinkedHashMap<String,Object> toJsonSplitContinuous(VfdtNodeSplitContinuous node) {
        LinkedHashMap<String, Object> json = new LinkedHashMap<>();
        json.put("type", node.getClass().getName());
        json.put("depth", node.getDepth());
        json.put("instanceCount", node.getInstanceCount());
        json.put("splitAttributeIndex", node.getSplitAttributeIndex());
        json.put("threshold", node.getThreshold());
        return json;
    }

    @Override
    public boolean visitSplitDiscrete(VfdtNodeSplitDiscrete node) {
        writeJson(toJsonSplitDiscrete(node), "node-info.json");
        return visitSplit(node);
    }

    public LinkedHashMap<String,Object> toJsonSplitDiscrete(VfdtNodeSplitDiscrete node) {
        LinkedHashMap<String, Object> json = new LinkedHashMap<>();
        json.put("type", node.getClass().getName());
        json.put("depth", node.getDepth());
        json.put("instanceCount", node.getInstanceCount());
        json.put("splitAttributeIndex", node.getSplitAttributeIndex());
        json.put("children", node.getChildren().size());

        json.put("childrenAttrs", node.getChildren().stream()
                .map(i -> i.attrValue).collect(Collectors.toList()));
        return json;
    }

    @Override
    public boolean visitLeaf(VfdtNodeLeaf node) {
        writeJson(toJsonLeaf(node), "node-info.json");
        return true;
    }

    public LinkedHashMap<String, Object> toJsonLeaf(VfdtNodeLeaf node) {
        LinkedHashMap<String, Object> json = new LinkedHashMap<>();
        json.put("type", node.getClass().getName());
        json.put("depth", node.getDepth());
        json.put("instanceCount", node.getInstanceCount());
        json.put("classValue", node.getClassValue());
        json.put("errorCount", node.getErrorRate());
        json.put("errorRate", node.getErrorRate());

        return json;
    }

    @Override
    public boolean visitGrowing(VfdtNodeGrowing node) {
        totalCount = Math.max(node.getInstanceCount(), totalCount);

        writeJson(toJsonGrowing(node), "node-info.json");

        saveClassTotals(node, node.getClassTotals());

        for (AttributeClassCount count : node.getAttributeCounts()) {
            saveAttribute(node, count);
        }

        return true;
    }

    public LinkedHashMap<String, Object> toJsonGrowing(VfdtNodeGrowing node) {
        LinkedHashMap<String, Object> json = new LinkedHashMap<>();
        json.put("type", node.getClass().getName());
        json.put("depth", node.getDepth());
        json.put("instanceCount", node.getInstanceCount());
        json.put("instanceCountClassTotal", node.getInstanceCountClassTotal());
        json.put("parentClass", node.getSplitter().getParentClass());
        json.put("parentErrorRate", node.getSplitter().getParentErrorRate());
        json.put("sinceLastProcess", node.getSplitter().getSinceLastProcess());
        json.put("splitConfidence", node.getSplitter().getSplitConfidence());
        json.put("totalGini", node.totalGini());
        json.put("hoeffdingBound", node.hoeffdingBound(!node.getConfig().useGini));
        json.put("pure", node.isPure());
        json.put("classValue", node.getClassValue());
        json.put("mostCommonClass", node.getMostCommonClass());
        json.put("errorRate", node.getErrorRate());
        json.put("leafifyIndex", node.getLeafifyIndex(totalCount));

        BitSet path = node.getSplitter().getPathActiveAttributes();
        json.put("pathActiveAttributes", IntStream.range(0, path.size())
                .filter(path::get)
                .boxed()
                .collect(Collectors.toList()));

        VfdtNodeGrowing.AttributesGiniIndex g = node.giniIndices(!node.getConfig().useGini);
        json.put("giniIndices", g.attributes.stream()
                .map(i -> toJsonGiniIndex(node, g, i))
                .collect(Collectors.toList()));

        List<Object> countList = new ArrayList<>();
        for (AttributeClassCount count : node.getAttributeCounts()) {
            countList.add(toJsonAttributeClassCount(node, count));
        }
        json.put("attributeCounts", countList);
        return json;
    }

    public LinkedHashMap<String, Object> toJsonGiniIndex(VfdtNodeGrowing node, VfdtNodeGrowing.AttributesGiniIndex g, AttributeClassCount.GiniIndex i) {
        LinkedHashMap<String,Object> json = new LinkedHashMap<>();
        if (i.sourceAttribute != null) {
            json.put("attrIndex", node.getAttributeCountIndex(i.sourceAttribute));
        }
        json.put("giniIndex", i.index);
        json.put("threshold", i.threshold);
        if (g.first == i) {
            json.put("rank", 1);
        } else if (g.second == i) {
            json.put("rank", 2);
        } else {
            json.put("rank", -1);
        }
        return json;
    }

    public LinkedHashMap<String, Object> toJsonAttributeClassCount(VfdtNodeGrowing node, AttributeClassCount count) {
        LinkedHashMap<String, Object> countJson = new LinkedHashMap<>();
        countJson.put("index", node.getAttributeCountIndex(count));
        countJson.put("kind", Character.toString(count.getKind()));
        countJson.put("class", count.getClass().getName());
        return countJson;
    }

    protected void write(Path path, Consumer<CsvWriter> f) {
        try (CsvWriter writer = new CsvWriter().withOutputFile(path)) {
            f.accept(writer);
        }
    }

    public void saveClassTotals(VfdtNodeGrowing node, LongCount count) {
        write(Paths.get(dirStack.peek()).resolve("classTotals.csv"), writer -> {
            writer.writeLine("#" + count.size(), "path", "classHash#" + count.rowSize(), "classNum", "classCount#" + count.total(), "classValue");
            saveClassTotals(writer, node, count, "0", 0, Collections.emptyList());
        });
    }

    //index, path, {items}, hash, clsNum, clsCount, clsValue
    private long saveClassTotals(CsvWriter writer, VfdtNodeGrowing node, LongCount count, String path, long index, List<String> items) {
        if (count == null) {
            return index;
        }

        for (int i = 0; i < count.rowSize(); ++i) {
            long cls = count.getArray(i);
            String clsCountStr;
            boolean hasCount;
            if (count instanceof LongCount.DoubleCount) {
                double clsCount = ((LongCount.DoubleCount) count).getArrayWithCountIndexDouble(i);
                clsCountStr = String.valueOf(clsCount);
                hasCount = (clsCount > 0);
            } else {
                long clsCount = count.getArrayWithCountIndex(i);
                clsCountStr = String .valueOf(clsCount);
                hasCount = (clsCount > 0);
            }

            if (hasCount) {
                String clsValue = classValue(node, cls);

                List<String> line = new ArrayList<>(items.size() + 6);
                line.addAll(ArraysAsList.get(String.valueOf(index), path));
                line.addAll(items);
                line.addAll(ArraysAsList.get(String.valueOf(i), String.valueOf(cls), clsCountStr, clsValue));
                writer.writeLine(line);
                ++index;
            }
        }

        index = saveClassTotals(writer, node, count.getLeft(), path + "0", index, items);
        index = saveClassTotals(writer, node, count.getRight(), path + "1", index, items);
        return index;
    }

    private String classValue(VfdtNodeGrowing node, long cls) {
        try {
            ArffAttributeType t = node.getTypes().get(node.getClassIndexInAttributeCounts());
            if (cls >= 0 && t.getKind() == ArffAttributeType.ATTRIBUTE_KIND_ENUM) {
                return ((ArffAttributeType.EnumAttributeType) t).getValues().get((int) cls);
            } else {
                return String.valueOf(cls);
            }
        } catch (Exception ex) {
            return cls + ":" + ex;
        }
    }


    public void saveAttribute(VfdtNodeGrowing node, AttributeClassCount count) {
        Path path = Paths.get(dirStack.peek()).resolve("attribute-" + node.getAttributeCountIndex(count) + ".csv");
        if (count instanceof AttributeContinuousClassCount) {
            AttributeContinuousClassCount contCount = (AttributeContinuousClassCount) count;
            write(path, (writer) -> {
                saveContinuous(node, contCount, writer);
            });
        } else if (count instanceof AttributeDiscreteClassCountArray) {
            AttributeDiscreteClassCountArray array = (AttributeDiscreteClassCountArray) count;
            write(path, (writer) -> {
                saveDiscrete(node, array, writer);
            });
        } else if (count instanceof AttributeDiscreteClassCountHash) {
            AttributeDiscreteClassCountHash hash = (AttributeDiscreteClassCountHash) count;
            write(path, (writer) -> {
                saveDiscrete(node, hash, writer);
            });
        } else if (count instanceof AttributeStringClassCount) {
            AttributeStringClassCount strCount = (AttributeStringClassCount) count;
            write(path, (writer) -> {
                saveString(node, strCount, writer);
            });
        }
    }

    protected void saveContinuous(VfdtNodeGrowing node, AttributeContinuousClassCount count, CsvWriter writer) {
        writer.writeLine("#", "path",  "lowerBound", "upperBounder", "count", "classHash#" + count.getClassTableSize(), "classNum", "classCount", "classValue");
        long index = saveContinuous(node, count, writer, count.getTotal(), -1, 0);
        int si = 0;
        for (AttributeContinuousSplit split : count.getSplits()) {
            index = saveContinuous(node, count, writer, split, si, index);
            ++si;
        }
    }

    protected long saveContinuous(VfdtNodeGrowing node, AttributeContinuousClassCount count, CsvWriter writer, AttributeContinuousSplit split, int splitIndex, long index) {
        boolean add = count.isAddingNewSplits();
        writer.writeLine(String.valueOf(index), splitIndex + (add ? "!" : "") +"#", String.valueOf(split.getLower()), String.valueOf(split.getUpper()), String.valueOf(split.getCount()),
                String.valueOf(split.getBoundaryClass()), String.valueOf(split.getBoundaryClassCount()), classValue(node, split.getBoundaryClass()));
        ++index;

        index = saveClassTotals(writer, node, split.getClassCount(), splitIndex + (add ? "!" : "") + "#", index, ArraysAsList.get(
                String.valueOf(split.getLower()), String.valueOf(split.getUpper()), String.valueOf(split.getClassCount().total())));


        return index;
    }

    protected void saveDiscrete(VfdtNodeGrowing node, AttributeDiscreteClassCountArray count, CsvWriter writer) {
        writer.writeLine("#", "attrNum#" + count.getAttributes(), "attrTotal", "classNum#" + count.getClasses(), "classCount", "attrValue", "classValue");
        long index = 0;
        for (LongCount.LongKeyValueIterator iter = count.attributeToClassesTotal(); iter.hasNext(); ) {
            long attrNum = iter.getKey();
            long countTotal = iter.nextValue();

            for (LongCount.LongKeyValueIterator clsIter = count.classToCount(attrNum); clsIter.hasNext(); ) {
                long clsNum = clsIter.getKey();
                long clsCount = clsIter.nextValue();

                writer.writeLine(String.valueOf(index), String.valueOf(attrNum), String.valueOf(countTotal),
                        String.valueOf(clsNum), String.valueOf(clsCount),
                        attrValue(node, count, attrNum), classValue(node, clsNum));
            }
        }
    }

    protected void saveDiscrete(VfdtNodeGrowing node, AttributeDiscreteClassCountHash count, CsvWriter writer) {
        writer.writeLine("#", "path", "attrHash#" + count.attrSize(), "attrNum", "classHash#" + count.clsSize(), "classNum", "classCount", "attrValue", "classValue");
        saveDiscreteHash(node, count, writer, "0", 0);
    }
    protected long saveDiscreteHash(VfdtNodeGrowing node, AttributeDiscreteClassCountHash count, CsvWriter writer, String path, long index) {
        if (count == null) {
            return index;
        }

        for (int i = 0; i < count.attrSize(); ++i) {
            for (int j = 0; j < count.clsSize(); ++j) {
                long attrNum = count.getArray(count.attrIndex(i));
                long clsNum = count.getArray(count.clsIndex(j));
                long clsCount = count.getArray(count.countIndex(i, j));

                if (clsCount > 0) {
                    List<String> line = new ArrayList<>();

                    line.add(String.valueOf(index));
                    line.add(path);
                    line.add(String.valueOf(i));
                    line.add(String.valueOf(attrNum));
                    line.add(String.valueOf(j));
                    line.add(String.valueOf(clsNum));
                    line.add(String.valueOf(clsCount));

                    line.add(attrValue(node, count, attrNum));
                    line.add(classValue(node, clsNum));

                    writer.writeLine(line);
                    ++index;
                }
            }
        }

        index = saveDiscreteHash(node, count.getLeftWithoutCreate(), writer, path + "0", index);
        index = saveDiscreteHash(node, count.getRightWithoutCreate(), writer, path + "1", index);

        return index;
    }

    protected String attrValue(VfdtNodeGrowing node, AttributeDiscreteClassCount count, long attr) {
        ArffAttributeType t = node.getTypes().get(node.getAttributeCountIndex(count));
        try {
            if (attr >= 0 && t.getKind() == ArffAttributeType.ATTRIBUTE_KIND_ENUM) {
                return ((ArffAttributeType.EnumAttributeType) t).getValues().get((int) attr);
            } else {
                return String.valueOf(attr);
            }
        } catch (Exception ex) {
            return attr + ":" + ex;
        }
    }

    protected void saveString(VfdtNodeGrowing node, AttributeStringClassCount count, CsvWriter writer) {
        Map<String,LongCount> c = count.getCount();
        writer.writeLine("#", "path", "entryIndex#" + c.size(), "classHash#" + count.getClassTableSize(),"classNum", "classCount", "attrValue", "classValue");

        int ei = 0;
        long index = 0;
        for (Map.Entry<String,LongCount> e : c.entrySet()) {
            index = saveClassTotals(writer, node, e.getValue(), ei + ":0", index,
                    Collections.singletonList(String.valueOf(ei)));
            ++ei;
        }

    }
}
