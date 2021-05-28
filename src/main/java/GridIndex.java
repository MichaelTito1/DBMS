import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.Vector;

public class GridIndex implements Serializable {
    private static final long serialVersionUID = 1L;
    private int maxRowsPerBucket;
    private String path;
    private int bucketCount = 0;
    private Vector<String> indexedRows;
    private Vector<Bucket> buckets; // todo: how to handle single/multidimensional indices ?!

    //todo: how to handle range of each bucket ?! 2 cases: when table is empty or has entries
    //todo: how to name the index and the buckets?

    public GridIndex(String path, int maxRowsPerBucket, String[] colNames) {
        this.path = path;
        this.maxRowsPerBucket = maxRowsPerBucket;

        buckets = new Vector<Bucket>();

        indexedRows = new Vector<String>();
        Collections.addAll(indexedRows, colNames); // copying column names
    }

    public Bucket createBucket() throws DBAppException {
        Bucket b = new Bucket(path+"_bucket"+bucketCount+".class", maxRowsPerBucket);
        //todo: set min and max range of the new bucket
        buckets.add(b);
        maxRowsPerBucket++;
        save();
        return b;
    }

    public void save() throws DBAppException {
        File f = new File(path+".class");

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
}
