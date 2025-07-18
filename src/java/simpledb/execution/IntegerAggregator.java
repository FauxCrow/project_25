package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    // Note: Using a map so
    private final Map<Field, Integer> aggregateMap;
    private final Map<Field, Integer> countMap;

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
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        aggregateMap = new HashMap<>();
        countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        IntField afieldVal = (IntField) tup.getField(afield);
        int value = afieldVal.getValue();

        aggregateMap.putIfAbsent(groupField, initialValue());
        countMap.put(groupField, countMap.getOrDefault(groupField, 0) + 1);

        switch (what) {
            case COUNT:
                aggregateMap.put(groupField, aggregateMap.get(groupField) + 1);
                break;
            case SUM:
                aggregateMap.put(groupField, aggregateMap.get(groupField) + value);
                break;
            case MIN:
                aggregateMap.put(groupField, Math.min(aggregateMap.get(groupField), value));
                break;
            case MAX:
                aggregateMap.put(groupField, Math.max(aggregateMap.get(groupField), value));
                break;
            case AVG:
                // sum is updated in aggregateMap, count in countMap
                aggregateMap.put(groupField, aggregateMap.get(groupField) + value);
                break;
        }
    }

    private int initialValue() {
        switch (what) {
            case MIN: return Integer.MAX_VALUE;
            case MAX: return Integer.MIN_VALUE;
            case COUNT:
            case SUM:
            case AVG:
            default: return 0;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> results = new ArrayList<>();
        TupleDesc td;

        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Field group = null;
            int result = computeAggregate(group);
            Tuple tup = new Tuple(td);
            tup.setField(0, new IntField(result));
            results.add(tup);
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            for (Field group : aggregateMap.keySet()) {
                Tuple tup = new Tuple(td);
                tup.setField(0, group);
                tup.setField(1, new IntField(computeAggregate(group)));
                results.add(tup);
            }
        }

        return new TupleIterator(td, results);
    }

    private int computeAggregate(Field group) {
        if (what == Op.AVG) {
            return aggregateMap.get(group) / countMap.get(group);
        }
        return aggregateMap.get(group);
    }
}
