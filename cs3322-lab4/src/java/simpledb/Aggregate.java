package simpledb;

import java.util.*;

import static simpledb.Aggregator.NO_GROUPING;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final int afield, gfield;
    private final Aggregator.Op aop;
    private DbIterator child;
    private DbIterator result = null;
    private TupleDesc td;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	    this.child = child;
	    this.afield = afield;
	    this.gfield = gfield;
	    this.aop = aop;
        this.td = createTupleDesc();
    }

    private TupleDesc createTupleDesc() {
        TupleDesc childTd = child.getTupleDesc();
        Type atype = childTd.getFieldType(afield);
        String aname =  "aggName(" + nameOfAggregatorOp(aop) + ") (" + childTd.getFieldName(afield) + ")";
        if (gfield == NO_GROUPING) {
            return new TupleDesc(new Type[]{atype}, new String[]{aname});
        } else {
            Type gtype = childTd.getFieldType(gfield);
            String gname = childTd.getFieldName(gfield);
            return new TupleDesc(new Type[]{gtype, atype}, new String[]{gname, aname});
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    if (gfield == NO_GROUPING) {
            return null;
        }
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    // when should this happen?
    private void doCalculation() throws TransactionAbortedException, DbException {
        Aggregator aggregator;
        Type gfieldtype = null;
        if (gfield != NO_GROUPING) {
            gfieldtype = child.getTupleDesc().getFieldType(gfield);
        }
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gfieldtype, afield, aop);
        } else {
            aggregator = new StringAggregator(gfield, gfieldtype, afield, aop);
        }
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();
        result = aggregator.iterator();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        doCalculation();
        result.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    if (result.hasNext()) {
	        return result.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    result.rewind();
        // ensure that Operator.next == null
        super.close();
        super.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    return td;
    }

    public void close() {
	    result.close();
	    super.close();
    }

    @Override
    public DbIterator[] getChildren() {
	    return new DbIterator[] {child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
        td = createTupleDesc();
    }
    
}
