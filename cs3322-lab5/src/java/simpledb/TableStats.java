package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * <p>
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {
	
	private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();
	
	static final int IO_COST_PER_PAGE = 1000;
	
	public static TableStats getTableStats(String tableName) {
		return statsMap.get(tableName);
	}
	
	public static void setTableStats(String tableName, TableStats stats) {
		statsMap.put(tableName, stats);
	}
	
	public static void setStatsMap(HashMap<String,TableStats> s) {
		try {
			java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
			statsMapF.setAccessible(true);
			statsMapF.set(null, s);
		} catch (NoSuchFieldException | SecurityException |
		         IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	public static Map<String, TableStats> getStatsMap() {
		return statsMap;
	}
	
	public static void computeStatistics() {
		Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();
		
		System.out.println("Computing table stats.");
		while (tableIt.hasNext()) {
			int tableid = tableIt.next();
			TableStats s = new TableStats(tableid, IO_COST_PER_PAGE);
			setTableStats(Database.getCatalog().getTableName(tableid), s);
		}
		System.out.println("Done.");
	}
	
	/**
	 * Number of bins for the histogram. Feel free to increase this value over
	 * 100, though our tests assume that you have at least 100 bins in your
	 * histograms.
	 */
	static final int NUM_HIST_BINS = 100;
	
	private int tupleCount;
	private final int numPages, ioCostPerPage;
	private final Histogram[] hist;
	private final HashMap<String, Integer> indexTbl = new HashMap<>();
	private final int[] min, max;
	
	public int getIndex(String name) {
		return name == null ? -1 : indexTbl.getOrDefault(name, -1);
	}
	
	/**
	 * Create a new TableStats object, that keeps track of statistics on each
	 * column of a table
	 *
	 * @param tableId
	 *            The table over which to compute statistics
	 * @param ioCostPerPage
	 *            The cost per page of IO. This doesn't differentiate between
	 *            sequential-scan IO and disk seeks.
	 */
	public TableStats(int tableId, int ioCostPerPage) {
		// For this function, you'll have to get the DbFile for the table in question,
		// then scan through its tuples and calculate the values that you need.
		// You should try to do this reasonably efficiently, but you don't
		// necessarily have to (for example) do everything in a single scan of the table.
		
		this.ioCostPerPage = ioCostPerPage;
		
		HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
		this.numPages = file.numPages();
		
		TupleDesc td = file.getTupleDesc();
		int numFields = td.numFields();
		this.hist = new Histogram[numFields];
		this.min = IntStream.generate(() -> Integer.MAX_VALUE)
				.limit(numFields)
				.toArray();
		this.max = IntStream.generate(() -> Integer.MIN_VALUE)
				.limit(numFields)
				.toArray();
		
		Transaction trans = new Transaction();
		DbFileIterator it = file.iterator(trans.getId());
		try {
			it.open();
			while (it.hasNext()) {
				this.tupleCount++;
				Tuple tup = it.next();
				
				IntStream.range(0, numFields)
						.filter(i -> tup.getField(i).getType() == Type.INT_TYPE)
						.forEach((i) -> {
							int val = ((IntField) tup.getField(i)).getValue();
							this.min[i] = Integer.min(this.min[i], val);
							this.max[i] = Integer.max(this.max[i], val);
						});
			}
			
			IntStream.range(0, numFields)
					.filter(i -> td.getFieldType(i) != null)
					.forEach((i) -> {
						if (td.getFieldType(i) == Type.INT_TYPE) {
							this.hist[i] = new IntHistogram(
									Integer.min(NUM_HIST_BINS, this.max[i] - this.min[i] + 1),
									this.min[i], this.max[i]);
						} else {
							this.hist[i] = new StringHistogram(NUM_HIST_BINS);
						}
					});
			
			it.rewind();
			while (it.hasNext()) {
				Tuple tup = it.next();
				IntStream.range(0, numFields)
						.filter(i -> td.getFieldType(i) != null)
						.forEach((i) -> {
							if (td.getFieldType(i) == Type.INT_TYPE) {
								int val = ((IntField) tup.getField(i)).getValue();
								((IntHistogram) this.hist[i]).addValue(val);
							} else {
								String val = ((StringField) tup.getField(i)).getValue();
								((StringHistogram) this.hist[i]).addValue(val);
							}
						});
			}
			
		} catch (DbException | TransactionAbortedException e) {
			e.printStackTrace();
		} finally {
			it.close();
		}
	}
	
	/**
	 * Estimates the cost of sequentially scanning the file, given that the cost
	 * to read a page is costPerPageIO. You can assume that there are no seeks
	 * and that no pages are in the buffer pool.
	 * <p>
	 * Also, assume that your hard drive can only read entire pages at once, so
	 * if the last page of the table only has one tuple on it, it's just as
	 * expensive to read as a full page. (Most real hard drives can't
	 * efficiently address regions smaller than a page at a time.)
	 *
	 * @return The estimated cost of scanning the table.
	 */
	public double estimateScanCost() {
		return this.numPages * this.ioCostPerPage;
	}
	
	/**
	 * This method returns the number of tuples in the relation, given that a
	 * predicate with selectivity selectivityFactor is applied.
	 *
	 * @param selectivityFactor
	 *            The selectivity of any predicates over the table
	 * @return The estimated cardinality of the scan with the specified
	 *         selectivityFactor
	 */
	public int estimateTableCardinality(double selectivityFactor) {
		return (int) (this.tupleCount * selectivityFactor);
	}
	
	/**
	 * The average selectivity of the field under op.
	 * @param field
	 *        the index of the field
	 * @param op
	 *        the operator in the predicate
	 * The semantic of the method is that, given the table, and then given a
	 * tuple, of which we do not know the value of the field, return the
	 * expected selectivity. You may estimate this value from the histograms.
	 * */
	public double avgSelectivity(int field, Predicate.Op op) {
		// TODO
		throw new UnsupportedOperationException("你好");
//		return 1.0;
	}
	
	/**
	 * Estimate the selectivity of predicate <tt>field op constant</tt> on the
	 * table.
	 *
	 * @param field
	 *            The field over which the predicate ranges
	 * @param op
	 *            The logical operation in the predicate
	 * @param constant
	 *            The value against which the field is compared
	 * @return The estimated selectivity (fraction of tuples that satisfy) the
	 *         predicate
	 */
	public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
		if (this.hist[field] instanceof IntHistogram)
			return ((IntHistogram) this.hist[field]).estimateSelectivity(op, ((IntField) constant).getValue());
		else if (this.hist[field] instanceof StringHistogram)
			return ((StringHistogram) this.hist[field]).estimateSelectivity(op, ((StringField) constant).getValue());
		
		throw new RuntimeException("亻尔女子口阿");
	}
	
	/**
	 * return the total number of tuples in this table
	 * */
	public int totalTuples() {
		return this.tupleCount;
	}
	
	public int getMin(int field) {
		return this.min[field];
	}
	
	public int getMax(int field) {
		return this.max[field];
	}
}
