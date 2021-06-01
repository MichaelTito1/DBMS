import java.io.*;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Map.Entry;

// Vector<Int, vector of indexed attributes>
public class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;
    private int maxRows;
    // attributes needed to locate a row
    private Hashtable<Object, Vector<Object> > rowAddress; // key: primary key of each row , value(s): indexed column value(s) of this row
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
        rowAddress = new Hashtable<Object, Vector<Object>>();
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

    public Hashtable<Object, Vector<Object>> getRowAddress() {
        return rowAddress;
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
     * this method inserts [page address, position of the row in its page, indexed columns' values from the row] in the bucket
     * @param row
     * @param htbl
     * @param pkPos
     */
    public void insert(Row row, Hashtable<String, String> htbl, int pkPos) throws DBAppException {
        // if this bucket is full, insert in the overflow bucket the new row [the easy answer] [recheck piazza @583]
        if(size >= maxRows){
            // if bucket is already created, insert directly
            if(nextBucket != null)
                nextBucket.insert(row,htbl,pkPos);
            else {
                // else if it doesn't exist, create it then insert.
                nextBucket = gi.createBucket(min, max);
                nextBucket.insert(row, htbl,pkPos);
            }
        }

        Vector<Object> filteredCols = filterIndexedCols(htbl, row); // get indexed columns' values only
        Object pkVal = row.getValue(pkPos);
        rowAddress.put(pkVal,filteredCols);
        // [OLD] if this page is NOT new to the bucket, insert directly
        // else if this page is new, initialize its hashtable then insert
        save();
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
     * given a primary key of the value to be deleted, this method deletes it from the bucket.
     * If not found in this bucket, it should call its overflow bucket's delete method
     * @param pk
     * @return deleted object if deleted, null if deletion failed for whatever reason.
     */
    public Vector<Object> delete(Object pk) throws DBAppException {
        Vector<Object> deleted = rowAddress.remove(pk); // attempt deletion
        if(deleted == null){ // if not found, delete it from the overflow bucket.
            deleted = nextBucket.delete(pk);
        }
        save();
        return deleted;
    }
}
