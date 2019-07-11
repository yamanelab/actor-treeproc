package csl.dataproc.vfdt.count;

import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.VfdtConfig;

import java.io.Serializable;

/**
 * describe mappings from attrValue:A -&gt; clsValue:C -&gt; count:long.
 * Note: the type of class value C is long.
 * <ul>
 *     <li> long increment(A attrValue, C clsValue)</li>
 *     <li> long get(A attrValue, C clsValue)</li>
 * </ul>
 */
public interface AttributeClassCount {
    default boolean isDiscrete() {
        return getKind() == ArffAttributeType.ATTRIBUTE_KIND_INTEGER;
    }
    default boolean isContinuous() {
        return getKind() == ArffAttributeType.ATTRIBUTE_KIND_REAL;
    }

    char getKind();

    /** @return GiniIndex(giniIndex, threshold, this) :
     *     the length of the returned array &gt;=1, and
     *      currently up to 2.
     *      the former is smaller index. */
    GiniIndex[] giniIndex(long nodeInstanceCount, int classes, boolean entropy);

    class AttributeIgnoreClassCount implements AttributeClassCount, Serializable {
        protected VfdtConfig config;

        static final long serialVersionUID = 1L;

        public AttributeIgnoreClassCount() {}

        public AttributeIgnoreClassCount(VfdtConfig config) {
            this.config = config;
        }

        @Override
        public char getKind() {
            return '?';
        }

        @Override
        public GiniIndex[] giniIndex(long nodeInstanceCount, int classes, boolean entropy) {
            return new GiniIndex[] {
                    new GiniIndex(config.initGiniIndex, 0, this)
            };
        }
    }

    class GiniIndex implements Serializable {
        public double index;
        /** optional */
        public double threshold = 0;

        public AttributeClassCount sourceAttribute;

        static final long serialVersionUID = 1L;

        public GiniIndex() {}

        public GiniIndex(double index, double threshold, AttributeClassCount sourceAttribute) {
            this.index = index;
            this.threshold = threshold;
            this.sourceAttribute = sourceAttribute;
        }
        public void set(double index, double threshold) {
            this.index = index;
            this.threshold = threshold;
        }
    }


}
