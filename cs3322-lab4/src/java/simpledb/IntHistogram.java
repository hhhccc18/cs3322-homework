package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    class Rectangle {
        int l;
        int r;
        int w;
        int h;

        Rectangle(int l, int r) {
            this.l = l;
            this.r = r;
            this.w = r - l + 1;
            this.h = 0;
        }
    }

    private final int buckets, min, max, w;
    private final Rectangle[] rectangles;
    private int total = 0;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = buckets;
    	this.min = min;
    	this.max = max;
    	int num = max - min + 1;
    	w = (num % buckets == 0) ? num / buckets : num / buckets + 1;
        rectangles = new Rectangle[buckets];
    	for (int i = 0; i < buckets; ++i) {
    	    int l = min + i * w;
    	    int r = (i == buckets - 1) ? max : l + w - 1;
    	    rectangles[i] = new Rectangle(l, r);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        rectangles[(v - min) / w].h += 1;
        total += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int i = (v - min) / w;
        switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    return 0;
                }
                return (((double) rectangles[i].h) / rectangles[i].w) / total;
            case GREATER_THAN:
                if (v < min) {
                    return 1;
                }
                if (v > max) {
                    return 0;
                }
                double s1 = (((double) (rectangles[i].r - v)) / rectangles[i].w) * rectangles[i].h;
                for (int j = i + 1; j < buckets; ++j) {
                    s1 += rectangles[j].h;
                }
                return s1 / total;
            case LESS_THAN:
                if (v < min) {
                    return 0;
                }
                if (v > max) {
                    return 1;
                }
                double s2 = (((double) (v - rectangles[i].l)) / rectangles[i].w) * rectangles[i].h;
                for (int j = i - 1; j >= 0; --j) {
                    s2 += rectangles[j].h;
                }
                return s2 / total;
            case LESS_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN, v);
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            default:
                assert(false);
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder builder = new StringBuilder(String.format("IntHistogram buckets:%d min:%d max:%d w:%d total:%d ", buckets, min, max, w, total));
        for (Rectangle rectangle : rectangles) {
            builder.append(String.format("[l:%d r:%d h:%d] ", rectangle.l, rectangle.r, rectangle.h));
        }
        return builder.toString();
    }
}
