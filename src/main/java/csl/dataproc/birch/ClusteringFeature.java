package csl.dataproc.birch;

import java.io.Serializable;
import java.util.Arrays;

public class ClusteringFeature implements Serializable, Cloneable {
    protected int entries;
    protected double[] linearSum;
    protected double squareSum;

    static final long serialVersionUID = 1L;

    public ClusteringFeature() {
        this(0);
    }

    public ClusteringFeature(int dimension) {
        this.entries = 0;
        linearSum = new double[dimension];
        squareSum = 0;
    }

    public ClusteringFeature(double[] data) {
        entries = 1;
        int len = data.length;
        this.linearSum = Arrays.copyOf(data, len);
        double squareSum = 0;
        for (double d : data) {
            squareSum += d * d;
        }
        this.squareSum = squareSum;
    }

    public ClusteringFeature(int entries, double[] linearSum, double squareSum) {
        this.entries = entries;
        this.linearSum = linearSum;
        this.squareSum = squareSum;
    }

    public ClusteringFeature copy() {
        try {
            ClusteringFeature cf = (ClusteringFeature) super.clone();
            cf.linearSum = Arrays.copyOf(linearSum, linearSum.length);
            return cf;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public int getEntries() {
        return entries;
    }

    public double[] getLinearSum() {
        return linearSum;
    }

    public double getSquareSum() {
        return squareSum;
    }

    public void clear() {
        entries = 0;
        Arrays.fill(linearSum, 0);
        squareSum = 0;
    }

    public int getDimension() {
        return linearSum.length;
    }

    public void add(ClusteringFeature cf) {
        int srcLen = cf.getDimension();
        updateDimension(srcLen);

        entries += cf.entries;
        double[] linearSum = this.linearSum;
        double[] cfLinearSum = cf.linearSum;
        for (int i = 0; i < srcLen; ++i) {
            linearSum[i] += cfLinearSum[i];
        }
        this.squareSum += cf.squareSum;
    }

    private int updateDimension(int anotherDim) {
        int d = getDimension();
        if (d < anotherDim) {
            this.linearSum = Arrays.copyOf(linearSum, anotherDim);
            return anotherDim;
        } else {
            return d;
        }
    }

    public void remove(ClusteringFeature cf) {
        int srcLen = cf.getDimension();
        updateDimension(srcLen);

        entries -= cf.entries;
        double[] linearSum = this.linearSum;
        double[] cfLinearSum = cf.linearSum;
        for (int i = 0; i < srcLen; ++i) {
            linearSum[i] -= cfLinearSum[i];
        }
        this.squareSum -= cf.squareSum;
    }

    public double[] getCentroid() {
        double[] linearSum = this.linearSum;
        int len = linearSum.length;
        double[] centroid = Arrays.copyOf(linearSum, len);
        int entries = this.entries;
        for (int i = 0; i < len; ++i) {
            centroid[i] /= (double) entries;
        }
        return centroid;
    }


    /** D0 without sqrt */
    public double centroidEuclidDistance(ClusteringFeature cf) {
        double[] linearSum = this.linearSum;
        int entries = this.entries;
        double[] cfLinearSum = cf.linearSum;
        int cfEntries = cf.entries;
        double d = 0;
        for (int i = 0, len = getDimension(); i < len; ++i) {
            double v = linearSum[i] / entries - cfLinearSum[i] / cfEntries;
            d += v * v;
        }
        return d;
    }


    /** D1 */
    public double centroidManhattanDistance(ClusteringFeature cf) {
        double[] linearSum = this.linearSum;
        int entries = this.entries;
        double[] cfLinearSum = cf.linearSum;
        int cfEntries = cf.entries;
        double d = 0;
        for (int i = 0, len = getDimension(); i < len; ++i) {
            d += Math.abs(linearSum[i] / entries - cfLinearSum[i] / cfEntries);
        }
        return d;
    }


    /** D2 */
    public double averageInterClusterDistance(ClusteringFeature cf) {
        double[] linearSum = this.linearSum;
        int entries = this.entries;
        double[] cfLinearSum = cf.linearSum;
        int cfEntries = cf.entries;
        double d = 0;
        d += squareSum;
        d += cf.squareSum;
        for (int i = 0, len = getDimension(); i < len; ++i) {
            d -= 2 * linearSum[i] * cfLinearSum[i];
        }
        d /= (double) (entries * cfEntries);
        return Math.sqrt(d);
    }

    /** D3 */
    public double averageIntraClusterDistance(ClusteringFeature cf) {
        double[] linearSum = this.linearSum;
        int entries = this.entries;
        double[] cfLinearSum = cf.linearSum;
        int cfEntries = cf.entries;
        double d = 0;
        d += squareSum;
        d += cf.squareSum;
        for (int i = 0, len = getDimension(); i < len; ++i) {
            d -= 2 * linearSum[i] * cfLinearSum[i];
        }
        d /= (double) ((entries + cfEntries) * (entries + cfEntries - 1));
        return Math.sqrt(d);
    }

    /** D4 */
    public double varianceIncreaseDistance(ClusteringFeature cf) {
        double[] linearSum = this.linearSum;
        int entries = this.entries;
        double[] cfLinearSum = cf.linearSum;
        int cfEntries = cf.entries;
        double d = 0;
        int len = getDimension();
        for (int i = 0; i < len; ++i) {
            double s = (linearSum[i] + cfLinearSum[i]) / (double) (entries + cfEntries);
            double v = linearSum[i] - s;
            d += v * v;
            double w = cfLinearSum[i] - s;
            d += w * w;

            double x = linearSum[i] - linearSum[i] / (double) entries;
            d -= x * x;

            double y = cfLinearSum[i] - cfLinearSum[i] / (double) cfEntries;
            d -= y * y;
        }
        return d;
    }

}
