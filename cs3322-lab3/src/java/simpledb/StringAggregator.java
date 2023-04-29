package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	private static final long serialVersionUID = 1L;
	
	private final int gf;
	private final Type gfType;
	private final int af;
	private final Aggregator.Op op;
	
	private final HashMap<Field, Result> tbl;
	
	private final TupleDesc resultTupleDesc;
	
	/**
	 * Aggregate constructor
	 * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param afield the 0-based index of the aggregate field in the tuple
	 * @param what aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */
	
	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		if (what != Op.COUNT)
			throw new IllegalArgumentException("StringAggregator only supports COUNT");
		
		this.gf = gbfield;
		this.gfType = gbfieldtype;
		this.af = afield;
		this.op = what;
		
		this.tbl = new HashMap<>();
		
		this.resultTupleDesc = this.gf == NO_GROUPING ?
				new TupleDesc(new Type[] { Type.INT_TYPE }) :
				new TupleDesc(new Type[] { this.gfType, Type.INT_TYPE });
		// since only supports COUNT
	}
	
	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		if (this.gf != NO_GROUPING) {
			if (tup.getField(this.gf).getType() != this.gfType)
				throw new IllegalArgumentException("Group-by field type mismatch");
		}
		
		Field g = this.gf == NO_GROUPING ? null : tup.getField(this.gf);
		String s = ((StringField) tup.getField(this.af)).getValue();
		
		if (!this.tbl.containsKey(g))
			this.tbl.put(g, new CountResult());
		
		this.tbl.get(g).add(s);
	}
	
	public void clear() {
		this.tbl.clear();
	}
	
	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal,
	 *   aggregateVal) if using group, or a single (aggregateVal) if no
	 *   grouping. The aggregateVal is determined by the type of
	 *   aggregate specified in the constructor.
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
				return StringAggregator.this.resultTupleDesc;
			}
			
			public void close() {
				this.it = null;
			}
		};
	}
	
	private abstract class Result {
		abstract void add(String other);
		abstract Field[] get();
		final Tuple getTuple(Field group) {
			ArrayList<Field> fields = new ArrayList<>();
			if (group != null)
				fields.add(group);
			fields.addAll(Arrays.asList(this.get()));
			
			return Tuple.fromArrayList(
					StringAggregator.this.resultTupleDesc,
					fields);
		}
	}
	
	private class CountResult extends Result {
		private int count = 0;
		
		void add(String other) {
			this.count++;
		}
		
		Field[] get() {
			return IntField.fromInts(this.count);
		}
	}
}
