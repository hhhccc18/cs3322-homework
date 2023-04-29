package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
	private static final long serialVersionUID = 1L;
	
	private final Aggregator.Op op;
	private DbIterator child;
	private final int af, gf;
	private final Aggregator aggregator;
	private DbIterator it;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * <p>
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param aggregateField
     *            The column over which we are computing an aggregate.
     * @param groupField
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aggregateOp
     *            The aggregation operator to use
     */
	public Aggregate(DbIterator child, int aggregateField, int groupField, Aggregator.Op aggregateOp) {
	    this.op = aggregateOp;
		this.child = child;
		this.af = aggregateField;
		this.gf = groupField;
		
		this.aggregator = this.getAggregateType() == Type.INT_TYPE ?
			new IntegerAggregator(this.gf, this.getGroupType(), this.af, this.op) :
			new StringAggregator(this.gf, this.getGroupType(), this.af, this.op);
		
		this.it = null;
    }
	
	private simpledb.Type getType(int field) {
		return this.child.getTupleDesc().getFieldType(field);
	}
	
	private simpledb.Type getAggregateType() {
		return this.getType(this.af);
	}
	
	/**
	 * @return the group-by field type, or null if gf is NO_GROUPING
	 * */
	private simpledb.Type getGroupType() {
		return this.gf == Aggregator.NO_GROUPING ? null : this.getType(this.gf);
	}

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
	public int groupField() {
		return this.gf;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
	public String groupFieldName() {
		return this.child.getTupleDesc().getFieldName(this.gf);
    }

    /**
     * @return the aggregate field
     * */
	public int aggregateField() {
		return this.af;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
	public String aggregateFieldName() {
		return this.child.getTupleDesc().getFieldName(this.af);
    }

    /**
     * @return return the aggregate operator
     * */
	public Aggregator.Op aggregateOp() {
		return this.op;
    }

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
    }

	@Override
	public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		super.open();
		
		this.aggregator.clear();
		this.child.open();
		while (this.child.hasNext())
			this.aggregator.mergeTupleIntoGroup(this.child.next());
		this.child.close();
		
		this.it = this.aggregator.iterator();
		this.it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		return this.it.hasNext() ? this.it.next() : null;
    }

	public void rewind() throws DbException, TransactionAbortedException {
		this.it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
	public TupleDesc getTupleDesc() {
		return this.child.getTupleDesc();
	}

	@Override
	public void close() {
		super.close();
		this.it.close();
	}
 
	public DbIterator[] getChildren() {
		return new DbIterator[] { this.child };
	}

	public void setChildren(DbIterator[] children) {
		if (children.length != 1)
			throw new IllegalArgumentException("Aggregate requires 1 child");
		
		this.child = children[0];
	}
    
}
