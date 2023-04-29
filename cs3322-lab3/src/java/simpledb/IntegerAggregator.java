package simpledb;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
	private static final long serialVersionUID = 1L;
	
	private final int gf;
	private final Type gfType;
	private final int af;
	private final Aggregator.Op op;
	
	private final HashMap<Field, Result> tbl;
	
	private final TupleDesc resultTupleDesc;
	
	/**
	 * Aggregate constructor
	 *
	 * @param groupByField
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param groupByFieldType
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */
	
	public IntegerAggregator(int groupByField, Type groupByFieldType, int afield, Op what) {
		this.gf = groupByField;
		this.gfType = groupByFieldType;
		this.af = afield;
		this.op = what;
		
		this.tbl = new HashMap<>();
		
		this.resultTupleDesc = this.gf == NO_GROUPING ?
				new TupleDesc(new Type[] { Type.INT_TYPE }) :
				new TupleDesc(new Type[] { this.gfType, Type.INT_TYPE });
	}
	
	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 *
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		if (this.gf != NO_GROUPING) {
			if (tup.getField(this.gf).getType() != this.gfType)
				throw new IllegalArgumentException("Group-by field type mismatch");
		}
		
		Field g = this.gf == NO_GROUPING ? null : tup.getField(this.gf);
		int a = ((IntField) tup.getField(this.af)).getValue();
		
		if (!this.tbl.containsKey(g))
			this.tbl.put(g, this.getEmptyResult());
		
		this.tbl.get(g).add(a);
	}
	
	public void clear() {
		this.tbl.clear();
	}
	
	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public DbIterator iterator() {
		return new DbIterator() {
			private Iterator<Map.Entry<Field, Result>> it = null;
			
			public void open() throws DbException, TransactionAbortedException {
				this.it = tbl.entrySet().iterator();
			}
			
			public boolean hasNext() throws DbException, TransactionAbortedException {
				return this.it != null && this.it.hasNext();
			}
			
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (!this.hasNext())
					throw new NoSuchElementException();
				
				Map.Entry<Field, Result> entry = this.it.next();
				return entry.getValue().getTuple(entry.getKey());
			}
			
			public void rewind() throws DbException, TransactionAbortedException {
				this.close();
				this.open();
			}
			
			public TupleDesc getTupleDesc() {
				return IntegerAggregator.this.resultTupleDesc;
			}
			
			public void close() {
				this.it = null;
			}
		};
	}
	
	private Result getEmptyResult() {
		if (this.op == Aggregator.Op.MIN)
			return new MinResult();
		if (this.op == Aggregator.Op.MAX)
			return new MaxResult();
		if (this.op == Aggregator.Op.SUM)
			return new SumResult();
		if (this.op == Aggregator.Op.AVG)
			return new AvgResult();
		if (this.op == Aggregator.Op.COUNT)
			return new CountResult();
		if (this.op == Aggregator.Op.SUM_COUNT)
			return new SumCountResult();
		if (this.op == Aggregator.Op.SC_AVG)
			return new SCAvgResult();
		
		throw new IllegalArgumentException("Unknown op: " + this.op);
	}
	
	private abstract class Result {
		abstract void add(int other);
		abstract Field[] get();
		final Tuple getTuple(Field group) {
			ArrayList<Field> fields = new ArrayList<>();
			if (group != null)
				fields.add(group);
			fields.addAll(Arrays.asList(this.get()));
			
			return Tuple.fromArrayList(
					IntegerAggregator.this.resultTupleDesc,
					fields);
		}
	};
	
	private class MinResult extends Result {
		private int min;
		
		MinResult() {
			this.min = Integer.MAX_VALUE;
		}
		
		void add(int other) {
			this.min = Math.min(this.min, other);
		}
		
		Field[] get() {
			return IntField.fromInts(this.min);
		}
	}
	
	private class MaxResult extends Result {
		private int max;
		
		MaxResult() {
			this.max = Integer.MIN_VALUE;
		}
		
		void add(int other) {
			this.max = Math.max(this.max, other);
		}
		
		Field[] get() {
			return IntField.fromInts(this.max);
		}
	}
	
	private class SumResult extends Result {
		private int sum;
		
		SumResult() {
			this.sum = 0;
		}
		
		void add(int other) {
			this.sum += other;
		}
		
		Field[] get() {
			return IntField.fromInts(this.sum);
		}
	}
	
	private class AvgResult extends Result {
		private int sum;
		private int count;
		
		AvgResult() {
			this.sum = 0;
			this.count = 0;
		}
		
		void add(int other) {
			this.sum += other;
			this.count++;
		}
		
		Field[] get() { // Why return an int?
			return IntField.fromInts(this.sum / this.count);
//			throw new NotImplementedException();
		}
	}
	
	private class CountResult extends Result {
		private int count;
		
		CountResult() {
			this.count = 0;
		}
		
		void add(int other) {
			this.count++;
		}
		
		Field[] get() {
			return IntField.fromInts(this.count);
		}
	}
	
	private class SumCountResult extends Result {
		private int sum, count;
		
		SumCountResult() {
			this.sum = 0;
			this.count = 0;
		}
		
		void add(int other) {
			this.sum += other;
			this.count++;
		}
		
		Field[] get() {
			return IntField.fromInts(this.sum, this.count);
		}
	}
	
	private class SCAvgResult extends Result {
		SCAvgResult() {
			throw new NotImplementedException();
		}
		
		void add(int other) {
			throw new NotImplementedException();
		}
		
		Field[] get() {
			throw new NotImplementedException();
		}
	}
}
