package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {
	
	private static class BlockDS {
		final int n, B; // 0-based
		final int[] a, b;
		int total = 0;
		
		BlockDS(int length) {
			this.n = length;
			this.B = (int) Math.ceil(Math.sqrt(length));
			
			this.a = new int[length];
			this.b = new int[(int) Math.ceil((double) length / B)];
		}
		
		void add(int i, int v) {
			a[i] += v;
			b[i / B] += v;
			total += v;
		}
		
		void add(int i) {
			add(i, 1);
		}
		
		int sum(int l, int r) {
			if (l / B == r / B)
				return Arrays.stream(a, l, r + 1).sum();
			
			return Arrays.stream(a, l, B * (l / B + 1)).sum()
					+ Arrays.stream(a, B * (r / B), r + 1).sum()
					+ Arrays.stream(b, l / B + 1, r / B).sum();
		}
		
		int get(int i) {
			return a[i];
		}
		
		int getTotal() {
			return total;
		}
	}
	
	private final int buckets, bucketSize, min, max;
	private final BlockDS ds;
	
	/**
	 * Create a new IntHistogram.
	 * <p>
	 * This IntHistogram should maintain a histogram of integer values that it receives.
	 * It should split the histogram into "buckets" buckets.
	 * <p>
	 * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
	 * <p>
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
		
		this.bucketSize = (int) Math.ceil((double) (max - min + 1) / buckets);
		this.ds = new BlockDS(Math.min(this.max - this.min + 1, this.buckets));
	}
	
	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		int i = (v - min) / bucketSize;
		this.ds.add(i);
	}
	
	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * <p>
	 * For example, if "op" is "GREATER_THAN" and "v" is 5,
	 * return your estimate of the fraction of elements that are greater than 5.
	 *
	 * @param op Operator
	 * @param v Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {
		if (v < this.min)
			return Arrays.asList(Predicate.Op.GREATER_THAN, Predicate.Op.GREATER_THAN_OR_EQ,
					Predicate.Op.NOT_EQUALS).contains(op) ? 1.0 : 0.0;
		else if (v > this.max)
			return Arrays.asList(Predicate.Op.LESS_THAN, Predicate.Op.LESS_THAN_OR_EQ,
					Predicate.Op.NOT_EQUALS).contains(op) ? 1.0 : 0.0;
		
		int i = (v - this.min) / this.bucketSize, cnt = 0;
		switch (op) {
			case EQUALS:
			case LIKE:
				cnt = this.ds.get(i);
				break;
			case GREATER_THAN:
				cnt = this.ds.sum(i + 1, this.ds.n - 1);
				break;
			case GREATER_THAN_OR_EQ:
				cnt = this.ds.sum(i, this.ds.n - 1);
				break;
			case LESS_THAN:
				cnt = this.ds.sum(0, i - 1);
				break;
			case LESS_THAN_OR_EQ:
				cnt = this.ds.sum(0, i);
				break;
			case NOT_EQUALS:
				cnt = this.ds.getTotal() - this.ds.get(i);
				break;
		}
		return (double) cnt / this.ds.getTotal();
	}
	
	/**
	 * @return
	 *     the average selectivity of this histogram.
	 * <p>
	 *     This is not an indispensable method to implement the basic
	 *     join optimization. It may be needed if you want to
	 *     implement a more efficient optimization
	 * */
	public double avgSelectivity() {
		throw new UnsupportedOperationException("你好");
//		return 1.0;
	}
	
	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		return "亻尔女子";
	}
}
