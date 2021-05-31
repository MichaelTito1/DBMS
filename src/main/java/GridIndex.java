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
     * 2. creates grid array dynamically.
     * 3. set the name scheme of the index.
     * 4. edit metadata info for indexed columns.
     * @param path
     * @param tableName
     * @param maxRowsPerBucket
     * @param colNames
     * @throws DBAppException
     */
    public GridIndex(String path, String tableName, String[] colNames, int maxRowsPerBucket) throws DBAppException {
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

        // edit the metadata.csv file accordingly
        editMetadata(tableName, indexedCols);
    }

    /**
     * edits indexed column info in metadata.csv file.
     * @param tableName
     * @param colNames
     * @throws DBAppException
     */
    private void editMetadata(String tableName, Vector<String> colNames) throws DBAppException {
        // load metadata.csv file from disk.
        String metadataPath = "src\\main\\resources\\metadata.csv";
        String line;
        FileWriter fw;
        BufferedReader br;

        try {
            br = new BufferedReader(new FileReader(metadataPath));
            fw = new FileWriter("metadata.csv");
            while( (line=br.readLine()) != null){
                String [] values = line.split(",");
                // if table name is not found in this line, append as is and continue.
                if( !(values[0].equals(tableName)) ){
                    fw.append(line);
                    continue;
                }
                // if column name in this line is found in the vector of indexed cols,
                // change indexed = true , then append and continue;
                if(colNames.contains(values[1])){
                    values[4] = "true";
                    String newLine = String.join(",", values); // append all array values, separated by comma
                    fw.append(newLine);
                }
            }
            fw.flush();
            fw.close();
            br.close();
        } catch (FileNotFoundException e) {
            throw new DBAppException(e.getMessage());
        } catch (IOException e1) {
            throw new DBAppException(e1.getMessage());
        }
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
        Bucket b = new Bucket(path+indexName+"_bucket"+bucketCount+".class", maxRowsPerBucket, indexedCols, this);
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

    /**
     * This method is used to insert the row in the correct position in the index
     * @param rowPagePos
     */
    public void insert(Vector<Object> rowPagePos, Hashtable<String,String> htbl){
        // does some wizardry to find the suitable bucket

        // 1.check if bucket is empty then insert
        // 2.if bucket is not empty and have space insert in the sorted place
        // 3.if bucket is full , check the index
        //  if it's the last row in bucket ,  create a overflow bucket and put in  it
        //  else put it in the sorted position and put the last row in an overflow bucket
        // when the bucket is found, pass the suitable attributes bucket.insert(row
        Bucket b;
        if(checkBucketAvailable()
    }
}
