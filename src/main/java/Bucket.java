import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

// Vector<Int, vector of indexed attributes>
public class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;
    private int maxRows;
    // attributes needed to locate a row
    private Vector<Reference> references;
    private Vector<String> indexedCols;
    private int size = 0;
    private Vector<Object> min;
    private Vector<Object> max; // min and max for each dimension [done]
    private String path;
    private Bucket nextBucket = null; // overflow bucket
    private GridIndex gi;

    /**
     * Constructor initializes the bucket attributes and saves it on disk
     * @param path path of saving this bucket
     * @param maxRows   maximum capacity of the bucket
     * @param indexedCols   vector of column names on which the index is created.
     * @throws DBAppException
     */
    public Bucket(String path, int maxRows, Vector<String> indexedCols, GridIndex gi) throws DBAppException {
        this.maxRows = maxRows;
        this.references = new Vector<Reference>();
        this.path = path;
        this.indexedCols = indexedCols;
        this.gi = gi;

        min = new Vector<Object>(indexedCols.size());
        max = new Vector<Object>(indexedCols.size());

        save();
    }

    public void setNextBucket(Bucket b) {
        this.nextBucket = b;
    }

    public Vector<Object> getMin() {
        return min;
    }

    public void setMin(Vector<Object> min) {
        this.min = min;
    }

    public Vector<Object> getMax() {
        return max;
    }

    public void setMax(Vector<Object> max) {
        this.max = max;
    }

    public int getMaxRows() {
        return maxRows;
    }

    public Vector<Reference> getReferences() {
        return references;
    }

    public void save() throws DBAppException {
        File f = new File(path);

        if(f.exists())
            f.delete();

        // Serializing the table
        ObjectOutputStream outStr;
        try {
            outStr = new ObjectOutputStream(new FileOutputStream(f));
            outStr.writeObject(this);
            outStr.close();
        } catch (FileNotFoundException e) {
            throw new DBAppException(e.getMessage());
        } catch (IOException e) {
            throw new DBAppException(e.getMessage());
        }
    }

    /**
     * this method inserts [page address, position of the row in its page, indexed columns' values from the row in the bucket
     * @param ref reference to be inserted in the bucket.
     */
    public void insert(Reference ref) throws DBAppException {
        // if this bucket is full, insert in the overflow bucket the new row [the easy answer] [recheck piazza @583]
        if(size >= maxRows){
            // if bucket is already created, insert directly
            if(nextBucket == null)
                nextBucket = gi.createBucket(min, max);
            nextBucket.insert(ref);
            save();
            return;
        }
        // get position of insertion in the bucket, then insert
        int pos = getInsertionPosition(ref);
        this.references.insertElementAt(ref, pos);
        save();
    }

    /**
     * Get insertion position of a reference in the bucket.
     * @param ref
     * @return
     */
    private int getInsertionPosition(Reference ref) {
        // Lower and upper bounds
        int n = this.references.size();
        int start = 0;
        int end = n - 1;

        // Traverse the search space
        while (start <= end)
        {
            int mid = (start + end) / 2;

            // If K is found
            Reference m = references.get(mid);
            int comp = m.compareTo(ref);
            if(comp == 0)
                return mid;

            else if (comp < 0)
                start = mid + 1;

            else
                end = mid - 1;
        }

        // Return insert position
        return end + 1;
    }

    /** Bucket : indexedCols, rowAddress
     *  Table: ay hashtable [3shan n3rf tarteeb el cols fel row]
     *  Row: array of objects
     *
     *  // takes a row, puts all indexed cols' values in a vector IN ORDER, returns this vector
     */
    private Vector<Object> filterIndexedCols(Hashtable<String, String> htbl, Row r){
        Vector<Object> result = new Vector<Object>();
        int i = 0;
        for(Entry<String, String> entry : htbl.entrySet()){
            String key = entry.getKey();
            if(indexedCols.contains(key))
                result.add(r.getValue(i));
            i++;
        }
        return result;
    }

    /**
     * given a reference of the value to be deleted, this method deletes it from the bucket.
     * If not found in this bucket, it should call its overflow bucket's delete method.
     * @param r1
     * @return deleted object if deleted, null if deletion failed for whatever reason.
     */
    public Reference delete(Reference r1) throws DBAppException {
        int n = references.size();
        // delete reference from this bucket
        for(int i = 0; i < n; i++){
            int comp = r1.compareTo(references.get(i));
            if(comp == 0){
                Reference del = references.remove(i);
                size--;
                save();
                return del;
            }
        }
        // if reference is not found, search for it in the overflow bucket.
        // if there is no overflow bucket return null.
        if(nextBucket == null)
            return null;
        return nextBucket.delete(r1);
    }

    /**
     * Given a primary key, the method searches for and deletes this primary key
     * from this bucket or overflow buckets.
     * @param targetPK
     * @return
     */
    public boolean deleteRow(Object targetPK) throws DBAppException {
        for(int i = size -1; i >= 0; i--){
            Reference ref = references.get(i);
            Object refPk = ref.getPrimaryKey();
            int comp = compareObjects(refPk, targetPK);
            if(comp == 0) // if this is the targeted reference, delete (this will search for it in .
                return references.remove(i) != null;
        }
        return nextBucket.deleteRow(targetPK);
    }

    public boolean deleteReference(int i) throws DBAppException {
        references.remove(i);
        size--;
        save();
        return true;
    }

    public Bucket getNextBucket() {
        return nextBucket;
    }

    // compares two objects according to their datatypes
    private int compareObjects(Object obj1, Object obj2) throws DBAppException{

        if( (obj1 instanceof java.lang.Integer) && (obj2 instanceof java.lang.Integer) ) {
            return ((Integer)obj1).compareTo((Integer) obj2);
        }
        else if( (obj1 instanceof java.lang.Double) && (obj2 instanceof java.lang.Double)){
            return ((Double)obj1).compareTo((Double) obj2);
        }
        else if( (obj1 instanceof java.lang.String) && (obj2 instanceof java.lang.String)){
            return ((String)obj1).compareTo((String) obj2);
        }
        else if( (obj1 instanceof java.util.Date) && (obj2 instanceof java.util.Date)){
            try {
                String newDate1 = convertDateFormat(obj1.toString());
                String newDate2 = convertDateFormat(obj2.toString());
                Date d1 = new SimpleDateFormat("yyyy-MM-dd").parse(newDate1);
                Date d2 = new SimpleDateFormat("yyyy-MM-dd").parse(newDate2);
                return d1.compareTo(d2);
            } catch (ParseException e) {
                throw new DBAppException(e.getMessage());
            }
        }
        else
            throw new DBAppException("These objects can't be compared as their types don't match.");

    }

    // this method converts date of format like Sun Oct 17 00:00:00 EET 1948
    // to yyyy-MM-dd
    private String convertDateFormat(String searchVal) throws DBAppException {
        String oldFormat = "EEE MMM dd HH:mm:ss zzz yyyy";
        String newFormat = "yyyy-MM-dd";
        String newDate;
        SimpleDateFormat sdf = new SimpleDateFormat(oldFormat);
        Date d1 = null;
        // checking first if the entered string is of the targeted format

        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat(newFormat);
            Date d2 = sdf1.parse(searchVal);
            return searchVal;
        } catch (ParseException e1) {
            try {
                d1 = sdf.parse(searchVal);
            } catch (ParseException e) {
                throw new DBAppException(e.getMessage());
            }
        }

        sdf.applyPattern(newFormat);
        newDate = sdf.format(d1);
        return newDate;
    }

    /**
     * Given a set of conditions, find all satisfying references in this bucket and overflow buckets.
     * @param htblColNameValue comparison conditions
     * @param logicalOp logical operator used
     * @return
     */
    public Vector<Reference> select(Hashtable<String, Object[]> htblColNameValue, String logicalOp) throws DBAppException {
        Vector<Reference> ans = new Vector<>();
        Vector<Boolean> condResults;
        for (Reference ref : references){

            condResults = evalConditionsOnReference(htblColNameValue, ref, logicalOp);
            boolean res = evalLogicalOperator(condResults, logicalOp);
            if(res)
                ans.add(ref);
        }
        ans.addAll(nextBucket.select(htblColNameValue, logicalOp));
        return ans;
    }

    /**
     * Given a set of comparison conditions and a reference, it checks if this reference meets all conditions.
     * @param htblColNameValue
     * @param ref
     * @param logicalOp
     * @return
     */
    private Vector<Boolean> evalConditionsOnReference(Hashtable<String, Object[]> htblColNameValue, Reference ref, String logicalOp) throws DBAppException {
        Vector<Boolean> ans = new Vector<>();
        for(Entry<String, Object[]> entry : htblColNameValue.entrySet()){
            String operator = (String) entry.getValue()[0];
            String column = entry.getKey();
            Object value = entry.getValue()[1];
            if(operator.equals(">"))
                ans.add( greater(column,ref, value));
            else if(operator.equals(">="))
                ans.add( greaterEqual(column,ref, value) );
            else if(operator.equals("<"))
                ans.add( smaller(column, ref, value) );
            else if(operator.equals("<="))
                ans.add( smallerEqual(column, ref, value) );
            else if(operator.equals("="))
                ans.add( equal(column, ref, value));
            else if(operator.equals("!="))
                ans.add( notEqual(column, ref, value) );
            else
                throw new DBAppException("Comparison operator "+ operator + " is invalid." );
        }
        return ans;
    }

    private Boolean greater(String column, Reference ref, Object value) throws DBAppException {
        int comp = compareObjects(ref.getRelevantValues().get(indexedCols.indexOf(column)), value);
        return comp > 0;
    }

    private Boolean greaterEqual(String column, Reference ref, Object value) throws DBAppException {
        int comp = compareObjects(ref.getRelevantValues().get(indexedCols.indexOf(column)), value);
        return comp >= 0;
    }

    private Boolean smaller(String column, Reference ref, Object value) throws DBAppException {
        int comp = compareObjects(ref.getRelevantValues().get(indexedCols.indexOf(column)), value);
        return comp < 0;
    }

    private Boolean smallerEqual(String column, Reference ref, Object value) throws DBAppException {
        int comp = compareObjects(ref.getRelevantValues().get(indexedCols.indexOf(column)), value);
        return comp <= 0;
    }

    private Boolean equal(String column, Reference ref, Object value) throws DBAppException {
        int comp = compareObjects(ref.getRelevantValues().get(indexedCols.indexOf(column)), value);
        return comp == 0;
    }

    private Boolean notEqual(String column, Reference ref, Object value) throws DBAppException {
        int comp = compareObjects(ref.getRelevantValues().get(indexedCols.indexOf(column)), value);
        return comp != 0;
    }

    /**
     * Given a vector of booleans and the logical operator [AND/OR/XOR], it applies the operator
     * on all elements of the vector and returns the result
     * @param condResults
     * @param logicalOp
     * @return
     */
    private boolean evalLogicalOperator(Vector<Boolean> condResults, String logicalOp) {
        if(logicalOp.toLowerCase(Locale.ROOT).equals("and"))
            return condResults.stream().reduce(Boolean::logicalAnd).orElse(false);
        else if(logicalOp.toLowerCase(Locale.ROOT).equals("or"))
            return condResults.stream().reduce(Boolean::logicalOr).orElse(false);

        return condResults.stream().reduce(Boolean::logicalXor).orElse(false);
    }
}
