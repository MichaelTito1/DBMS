import java.io.*;
import java.util.Hashtable;
import java.util.Vector;
import java.util.Map.Entry;

// Vector<Int, vector of indexed attributes>
public class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;
    private int maxRows;
    // attributes needed to locate a row
    private Hashtable<String, Hashtable<Integer, Vector<Object>> > rowAddress; // hashtable contains page address and row number(s), relevant column values of each row
    private Vector<String> indexedCols;
    private int size = 0;
    private Vector<Object> min;
    private Vector<Object> max; // min and max for each dimension [done]
    private String path;

    /**
     * Constructor initializes the bucket attributes and saves it on disk
     * @param path path of saving this bucket
     * @param maxRows   maximum capacity of the bucket
     * @param indexedCols   vector of column names on which the index is created.
     * @throws DBAppException
     */
    public Bucket(String path, int maxRows, Vector<String> indexedCols, GridIndex gi) throws DBAppException {
        this.maxRows = maxRows;
        rowAddress = new Hashtable<String, Hashtable<Integer, Vector<Object>>>();
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

    public Hashtable<String, Hashtable<Integer, Vector<Object>>> getRowAddress() {
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
    //todo: 1. Create bucket
    //todo: 2. save bucket
    //todo: 3. bucket + page ?!!
}
