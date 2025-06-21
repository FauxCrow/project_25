package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    // fix: contains all the field TDItems included in this TupleDesc
    private List<TDItem> items;

    /**
     * @return
     *         An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *                array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *                array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // Guard clause: input length is not equal
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("Type and field arrays should be of the same length.");
        }
        // Guard clause: typeAr does not contain at least one entry
        if (typeAr.length < 1) {
            throw new IllegalArgumentException("Type array should contain at least one entry.");
        }

        items = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++) {
            items.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *               array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // Guard clause: typeAr does not contain at least one entry
        if (typeAr.length < 1) {
            throw new IllegalArgumentException("Type array should contain at least one entry.");
        }

        items = new ArrayList<>();

        for (Type t : typeAr) {
            items.add(new TDItem(t, null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *          index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // Guard clause: i is not a valid field reference
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException("Invalid field index: " + i);
        }

        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *          The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *                                if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // Guard clause: i is not a valid field reference
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException("Invalid field index: " + i);
        }

        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *             name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *                                if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // Guard clause: no name has been given
        if (name == null) {
            throw new NoSuchElementException("Field name is null");
        }

        for (int i = 0; i < items.size(); i++) {
            String fieldName = items.get(i).fieldName;
            if (name.equals(fieldName)) {
                return i;
            }
        }

        // Guard clause: no field with a matching name is found
        throw new NoSuchElementException("Field name '" + name + "' not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;

        for (TDItem item : items) {
            totalSize += item.fieldType.getLen();
        }

        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        Iterator<TDItem> td1List = td1.iterator();
        while (td1List.hasNext()) {
            TDItem item = td1List.next();
            typeList.add(item.fieldType);
            nameList.add(item.fieldName);
        }

        Iterator<TDItem> td2List = td2.iterator();
        while (td2List.hasNext()) {
            TDItem item = td2List.next();
            typeList.add(item.fieldType);
            nameList.add(item.fieldName);
        }

        return new TupleDesc(typeList.toArray(new Type[0]), nameList.toArray(new String[0]));
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *          the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // Guard clause: TupleDescs are identical
        if (this == o){
            return true;
        }
        
        // Guard clause: o is null or o is not a TupleDesc
        if (o == null || !(o instanceof TupleDesc))
            return false;

        TupleDesc other = (TupleDesc) o;

        // Guard clause: TupleDescs do not have the same number of items
        if (this.numFields() != other.numFields()){
            return false;
        }

        // Guard clause: the i-th type in this TupleDesc is not equal to the i-th type in o for every i
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(other.getFieldType(i))) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Objects.hash(items.stream().map(td -> td.fieldType).toArray());
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < items.size(); i++) {
            TDItem item = items.get(i);
            sb.append(item.fieldType).append("(").append(item.fieldName).append(")");

            if (i < items.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
