package csl.dataproc.vfdt.count;

import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.VfdtConfig;
import csl.dataproc.vfdt.VfdtNodeGrowing;

/** attr:long -&gt; class:long -&gt; count:long */
public interface AttributeDiscreteClassCount extends AttributeClassCount {
    long increment(long attr, long cls);
    long get(long attr, long cls);

    @Override
    default char getKind() {
        return ArffAttributeType.ATTRIBUTE_KIND_INTEGER;
    }

    LongCount.LongKeyValueIterator attributeToClassesTotal();
    LongCount.LongKeyValueIterator classToCount(long attr);

    static AttributeDiscreteClassCount getFixed(VfdtConfig config, int attributes, int classes) {
        //consider "?" value
        ++attributes;
        ++classes;

        long n = ((long) attributes) * classes;
        if (n <= config.discreteClassCountArrayMax) {
            return new AttributeDiscreteClassCountArray(attributes, classes);
        } else {
            return new AttributeDiscreteClassCountHash(config);
        }
    }

    /** "core/ExampleGroupStats.c:ExampleGroupStatsGiniDiscreteAttributeSplit" */
    default GiniIndex[] giniIndex(long nodeInstanceCount, int classes, boolean entropy) {
        return new GiniIndex[] {
                new GiniIndex(computeGiniIndex(nodeInstanceCount, classes, entropy), 0, this)};
    }

    /** "core/ExampleGroupStats.c:ExampleGroupStatsGiniDiscreteAttributeSplit"
     * "core/ExampleGroupStats.c:ExampleGroupStatsEntropyDiscreteAttributeSplit" */
    default double computeGiniIndex(long nodeInstanceCount, int classes, boolean entropy) {
        double out = 0;
        boolean hasAttr = false;
        for (LongCount.LongKeyValueIterator iter = attributeToClassesTotal(); iter.hasNext(); ) {
            long attrValue = iter.getKey();
            double attrClsTotal = (double) iter.nextValue();
            hasAttr = true;

            double gini = entropy ? 0 : 1;

            for (LongCount.LongKeyValueIterator clsIter = classToCount(attrValue); clsIter.hasNext(); ) {
                double attrClsCount = clsIter.nextValue();

                double v = attrClsCount / attrClsTotal;
                if (entropy) {
                    if (v > 0) {
                        gini -= v * VfdtNodeGrowing.lg(v);
                    }
                } else {
                    gini -= v * v;
                }
            }
            out += (attrClsTotal / (double) nodeInstanceCount) * gini;
        }
        if (!hasAttr) {
            return entropy ? VfdtNodeGrowing.lg(classes) : 1.0;
        } else {
            return out;
        }
    }
}