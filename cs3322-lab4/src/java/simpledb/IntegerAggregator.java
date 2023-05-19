package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field DEFAULT_FIELD = new IntField(0);

    class Pair {
        int first;
        int second;

        Pair(int first) {
            this.first = first;
        }

        Pair(int first, int second) {
            this.first = first;
            this.second = second;
        }
    }

    private final int gbfield, afield;
    private final TupleDesc td;
    private final Op what;
    private final HashMap<Field, Pair> groups = new HashMap<>();

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        if (gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field field = tup.getField(afield);
        assert(field instanceof IntField);
        int value = ((IntField) field).getValue();
        Field key;
        if (gbfield == NO_GROUPING) {
            key = DEFAULT_FIELD;
        } else {
            key = tup.getField(gbfield);
        }
        if (groups.containsKey(key)) {
            Pair pair = groups.get(key);
            switch (what) {
                case MIN:
                    if (value < pair.first) {
                        pair.first = value;
                    }
                    break;
                case MAX:
                    if (value > pair.first) {
                        pair.first = value;
                    }
                    break;
                case SUM:
                    pair.first += value;
                    break;
                case AVG:
                    pair.first += value;
                    pair.second += 1;
                    break;
                case COUNT:
                    pair.first += 1;
                    break;
                default:
                    assert(false);
            }
        }  else {
            Pair pair = null;
            switch (what) {
                case MIN:
                case MAX:
                case SUM:
                    pair = new Pair(value);
                    break;
                case AVG:
                    pair = new Pair(value, 1);
                    break;
                case COUNT:
                    pair = new Pair(1);
                    break;
                default:
                    assert(false);
            }
            groups.put(key, pair);
        }
    }


    private int getResult(Pair pair) {
        return (what == Op.AVG) ? pair.first / pair.second : pair.first;
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
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfield == NO_GROUPING) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(getResult(groups.get(DEFAULT_FIELD))));
            tuples.add(tuple);
        } else {
            for (Map.Entry<Field, Pair> entry : groups.entrySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(getResult(entry.getValue())));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
