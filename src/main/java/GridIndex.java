import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Vector;

public class GridIndex implements Serializable {
    private static final long serialVersionUID = 1L;
    private int maxRowsPerBucket;
    private String path;
    private String tableName;
    private String indexName;
    private int bucketCount = 0;
    private Object [] grid;
    private Vector<String> indexedCols;
//    private Vector<Bucket> buckets; // how to handle single/multidimensional indices ?! [done]

    // how to handle range of each bucket ?! 2 cases: when table is empty or has entries [done]
    // how to name the index and the buckets? use tablename+colnames !! [done]
    //todo: Synchronize index with the table upon creation. Insert function needed!
    /**
     * Constructor:
     * 1. assigns index path, max row capacity of each bucket and indexed columns
     * 2. creates grid array dynamically
     * 3. set the name scheme of the index
     * @param path
     * @param tableName
     * @param maxRowsPerBucket
     * @param colNames
     * @throws DBAppException
     */
    public GridIndex(String path, String tableName, int maxRowsPerBucket, String[] colNames) throws DBAppException {
        this.path = path;
        this.tableName = tableName;
        this.maxRowsPerBucket = maxRowsPerBucket;

        // defining naming scheme of the index
        indexName = tableName;
        for(String col : indexedCols)
            indexName += "_" + col;

        // copying column names
        indexedCols = new Vector<String>();
        Collections.addAll(indexedCols, colNames);

        // dynamically creating a grid
        final int[] dimensions = new int[colNames.length];
        Arrays.fill(dimensions, 10);
        grid = (Object[]) Array.newInstance(Object.class, dimensions);

        // save the created index
        save();
    }

    /**
     * It returns a newly created bucket with the conventional naming scheme.
     * It sets its min and max boundaries for each dimension.
     *
     * @param min   min value in the bucket for each dimension
     * @param max   max value in the bucket for each dimension
     * @return  newly created bucket.
     * @throws DBAppException
     */
    public Bucket createBucket(Vector<Object> min, Vector<Object> max) throws DBAppException {
        Bucket b = new Bucket(path+indexName+"_bucket"+bucketCount+".class", maxRowsPerBucket, indexedCols);
        bucketCount++;
        b.setMin(min);
        b.setMax(max);
        save();
        return b;
    }

    public void save() throws DBAppException {
        File f = new File(path+"\\"+indexName+".class");

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
