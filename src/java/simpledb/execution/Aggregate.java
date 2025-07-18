package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    // Note: Aggregator can store either IntegerAggregator or StringAggregator
    private Aggregator aggregator;
    private OpIterator aggResults;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        // Note: We want to check type of afield so we can store a reference to integer
        // / string aggregator
        // This allows us to call their functions later, and whatever difference we
        // implement in the separate classes can be used in this universal code!!
        Type gbFieldType = (gfield == Aggregator.NO_GROUPING) ? null : child.getTupleDesc().getFieldType(gfield);
        Type aFieldType = child.getTupleDesc().getFieldType(afield);

        if (aFieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, gbFieldType, afield, aop);
        } else {
            this.aggregator = new StringAggregator(gfield, gbFieldType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if (gfield == Aggregator.NO_GROUPING) {
            return Aggregator.NO_GROUPING;
        }

        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        if (gfield == Aggregator.NO_GROUPING) {
            return null;
        }

        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        aggResults = aggregator.iterator();
        aggResults.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggResults != null && aggResults.hasNext()) {
            return aggResults.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggResults.rewind();
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
        Type aggType = Type.INT_TYPE; // since aggregation outputs ints
        String aggName = aop.toString() + "(" + child.getTupleDesc().getFieldName(afield) + ")";

        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[] { aggType }, new String[] { aggName });
        } else {
            Type gbType = child.getTupleDesc().getFieldType(gfield);
            String gbName = child.getTupleDesc().getFieldName(gfield);
            return new TupleDesc(new Type[] { gbType, aggType }, new String[] { gbName, aggName });
        }
    }

    public void close() {
        super.close();
        child.close();
        if (aggResults != null) {
            aggResults.close();
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length > 0) {
            this.child = children[0];
        }
    }
}
