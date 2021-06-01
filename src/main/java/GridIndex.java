import java.io.*;
import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GridIndex implements Serializable {
    private static final long serialVersionUID = 1L;
    private int maxRowsPerBucket;
    private String path;
    private String tableName;
    private String indexName;
    private int bucketCount = 0;
    private Object [] grid;
    private Vector<String> indexedCols; // vector names of indexed columns [id, age]
    private Vector<String> colTypes;    // vector of datatypes of indexed columns
    private Vector<Object> colMin;      // vector of minimum size of each indexed column IN ORDER
    private Vector<Object> colMax;      // vector of maximum size of each indexed column IN ORDER
    // key = column name, Value: a vector of size 10, each element contains its upper boundary. It should respect column datatypes.
    private Hashtable<String, Vector<Object>> htblRanges;
    private boolean pkIndexed = false;  // a flag to indicate if this index is created on the primary key
    private int pkPos;

    // how to handle single/multidimensional indices ?! [done]
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
    public GridIndex(String path,
                     String tableName,
                     String[] colNames,
                     int maxRowsPerBucket,
                     Hashtable<String,String> htblColTypes,
                     Hashtable<String, String> htblColMin,
                     Hashtable<String,String> htblColMax,
                     int pkPos) throws DBAppException {
        this.path = path;
        this.tableName = tableName;
        this.maxRowsPerBucket = maxRowsPerBucket;
        this.colMin = new Vector<Object>();
        this.colMax = new Vector<Object>();
        this.pkPos = pkPos;

        // defining naming scheme of the index
        indexName = tableName;
        for(String col : indexedCols) {
            indexName += "_" + col;
            //students_id_age
        }

        // copying column names
        indexedCols = new Vector<String>();
        Collections.addAll(indexedCols, colNames);

        // copying column datatypes IN ORDER, then
        // copying column min & maximum values IN ORDER
        this.colTypes = new Vector<String>();
        for(String col : indexedCols) {
            String type = htblColTypes.get(col);
            String minVal = htblColMin.get(col);
            String maxVal = htblColMax.get(col);
            colTypes.add(type);
            addMinMax(col, type, minVal, maxVal);
        }

        // creating column range vectors
        this.htblRanges = new Hashtable<String, Vector<Object>>();
        for(String col : indexedCols)
            htblRanges.put(col, new Vector<Object>(10));

        // dynamically creating a grid
        final int[] dimensions = new int[colNames.length];
        Arrays.fill(dimensions, 10);
        grid = (Object[]) Array.newInstance(Object.class, dimensions);

        // divide each dimension into 10 divisions
        createDivisions();

        // edit the metadata.csv file accordingly
        editMetadata(tableName, indexedCols);

        // save the created index
        save();
    }

    /**
     *  this method is used to parse the min & max values of each column and add
     *  them to their respective vectors.
     * @param colName
     * @param type
     * @param minVal
     * @param maxVal
     */
    private void addMinMax(String colName, String type, String minVal, String maxVal) throws DBAppException {
        if (type.equals("java.lang.Integer")){ // parse integer
            colMin.add(Integer.parseInt(minVal));
            colMax.add(Integer.parseInt(maxVal));
        }
        else if (type.equals("java.lang.Double")){ // parse double
            colMin.add(Double.parseDouble(minVal));
            colMax.add(Double.parseDouble(maxVal));
        }
        else if (type.equals("java.util.Date")) { // parse date
            Date minD = convertStringToDate(minVal);
            Date maxD = convertStringToDate(maxVal);
            colMin.add(minD);
            colMax.add(maxD);
        }
        else{ // in case of string add directly
            colMin.add(minVal);
            colMax.add(maxVal);
        }
    }

    /**
     * this method converts a string to date format "yyyy-MM-dd"
     * @param x string to be converted
     * @return
     * @throws DBAppException
     */
    private Date convertStringToDate(String x) throws DBAppException {
        Date d;
        try {
            String newDate = convertDateFormat(x);
            d = (Date) (new SimpleDateFormat("yyyy-MM-dd").parse(newDate));
        } catch (ParseException e) {
            throw new DBAppException(e.getMessage());
        }
        return d;
    }

    /**
     * This method initializes each column's divisions according to its datatype [integer, double, string, date]
     */
    private void createDivisions(){
        int n = indexedCols.size();
        for(int i = 0; i < n; i++){
            String type = colTypes.get(i);
            if(type.equals("Integer"))
                integerDivisions(i);
            else if(type.equals("Double"))
                doubleDivisions(i);
            else if(type.equals("String"))
                stringDivisions();
            else
                dateDivisions(i);
        }
    }

    /**
     * This method initializes the divisions of a column whose type is integer
     * @param i the position of the column in vector indexedCols
     */
    private void integerDivisions(int i){
        String colName = indexedCols.get(i);             // column name
        int delta = Integer.parseInt(colMax.get(i))/10; // delta is how big each unit of divisions is (maximumValue/10)
        Vector<Object> range = htblRanges.get(colName); // vector that holds the ranges
        for(int j = 0; j < 10; j++)
            range.set(j, (j+1)*delta); // upper bound of this cell = (j+1)*(maximumValue/10)
    }

    private void doubleDivisions(int i){
        String colName = indexedCols.get(i);             // column name
        double delta = Double.parseDouble(colMax.get(i))/10.0; // delta is how big each unit of divisions is (maximumValue/10)
        Vector<Object> range = htblRanges.get(colName); // vector that holds the ranges
        for(int j = 0; j < 10; j++)
            range.set(j, ((double)j+1.0) *delta); // upper bound of this cell = (j+1)*(maximumValue/10)
    }

    private void stringDivisions(int i){

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

    /**
     * It saves the grid index in the following naming scheme
     * [tableName]_[indexed col1]_[indexed col2].class
     * There may be one or more columns
     * @throws DBAppException
     */
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

    public boolean checkBucketAvailable (Vector<Object> rowPagePos ,Vector<String> indexedCols ){

        /*if(bucketCount == 0) {
            // create new bucket
        }
        String x = tableName + "" + indexedCols;
        while(bucketCount != 0) {
            if (x.equals(indexName))
                return true;

        }
        for(int i = 0; i < bucketCount ; i++){

        }*/

    }

    /**
     * This method is used to insert the row in the correct position in the index
     * @param rowPagePos
     */
    public void insert(Vector<Object> rowPagePos, Hashtable<String,String> htbl) throws DBAppException {
        // does some wizardry to find the suitable bucket
        Row r = (Row) rowPagePos.get(0);
        Vector<Integer> filtered = filterIndexedCols(htbl, r);
        Vector<Integer> gridPos = new Vector<Integer>();

        for(Integer pos : filtered){
            String colName = r.getColNames().get(pos);
            Vector<Object> range = htblRanges.get(colName); // max age 30 : [3,6,9,12,...,30] insert 9
            int n = range.size();
            Object colVal = r.getValue(pos);

            for(int i = 0; i < n; i++){
                Object div = range.get(i);
                int res = compareObjects(div, colVal);
                if(res > 0){
                    // if division upper boundary is greater than the column value, save division's
                    // position and exit the loop.
                    gridPos.add(i);
                    break;
                }
            }
        }

        // narrowing down on dimensions until we reach the last dimension which will give
        // us the bucket in which we should insert
        int n = gridPos.size();
        Object[] x = (Object[]) grid[gridPos.get(0)];
        for(int i = 1; i < n-1; i++)
            x = (Object[]) x[gridPos.get(i)];
        Bucket targetBucket = (Bucket) x[gridPos.get(n-1)];
        if(targetBucket == null)
            targetBucket = createBucket(colMin, colMax);
        targetBucket.insert(r, htbl, pkPos);

        save();
    }

    /**
     * This gets the target cell on which we should do an operation [insert/update/delete/select]
     * @param r
     * @param filtered
     * @return
     * @throws DBAppException
     */
    private Bucket getTargetCell(Row r, Vector<Integer> filtered,) throws DBAppException {
        Vector<Integer> gridPos = new Vector<Integer>();

        for(Integer pos : filtered){
            String colName = r.getColNames().get(pos);
            Vector<Object> range = htblRanges.get(colName); // max age 30 : [3,6,9,12,...,30] insert 9
            int n = range.size();
            Object colVal = r.getValue(pos);

            for(int i = 0; i < n; i++){
                Object div = range.get(i);
                int res = compareObjects(div, colVal);
                if(res > 0){
                    // if division upper boundary is greater than the column value, save division's
                    // position and exit the loop.
                    gridPos.add(i);
                    break;
                }
            }
        }

        // narrowing down on dimensions until we reach the last dimension which will give
        // us the bucket in which we should insert
        int n = gridPos.size();
        Object[] x = (Object[]) grid[gridPos.get(0)];
        for(int i = 1; i < n-1; i++)
            x = (Object[]) x[gridPos.get(i)];

        Bucket targetBucket = (Bucket) x[gridPos.get(n-1)];
        if(targetBucket == null)
            targetBucket = createBucket(colMin, colMax);
        return targetBucket;
    }

    private Vector<Integer> filterIndexedCols(Hashtable<String, String> htbl, Row r){
        Vector<Integer> result = new Vector<Integer>();
        int i = 0;
        for(Map.Entry<String, String> entry : htbl.entrySet()){
            String key = entry.getKey();
            int pos = indexedCols.indexOf(key);
            if(pos != -1)
                result.add(pos);
            i++;
        }
        return result;
    }

    // compares two rows according to their primary keys
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
     * This performs delete function on a given row
     */
    public void delete(Hashtable<String, Object> htblColNameValue){
        // 1. find the list of common columns
        Set<String> htblCols = htblColNameValue.keySet();
        Vector<String> commonCols = new Vector<String>();
        for(String key : indexedCols){
            if(htblCols.contains(key))
                commonCols.add(key);
        }

        int n = commonCols.size();

        if(n == 0){ // CASE UNKNOWN ?!!?!?!?!?!

        }
        else{
            // loop fel commonCols to find the approp. division for each col
            for(int i = 0; i < n; i++){
                String key = commonCols.get(i);

            }
        }
    }
    public void updateUsingIndex(String pkvalue, Hashtable<String,Object> htblupdate ,Vector<String> indexedCols){
        //update(pkvalue,htblupdate);

        if(!indexedCols.contains(pkvalue)){
            //call the old update method
        }
        else {
            //int i = indexedCols.indexOf(pkvalue);
            for (Map.Entry<String, Object> entry : htblupdate.entrySet()) {
                String colName = entry.getKey();
                Object colValue = entry.getValue();

                int colIdx = indexedCols.get();
                //Row updatedRow = curPage.getRow();
                //updatedRow.addValue(colIdx, colValue);

            }

        }
    }
}


/* INSERT PSEUDOCODE
 assume eno indexed columns contain the dimensions in the same order
 1. filter indexed cols from rowPagePos
 2. for each column:
        a. fetch el range bta3o mn htblRanges
        b. compare el value eli m3ak bel divisions [linearly] until you find the suitable division [comparison depends on datatype]
        c. save the position of the suitable division
 3. use the obtained positions IN ORDER to insert in the appropriate grid cell bucket
 4. if the bucket doesn't exist, create it. Else, bucket.insert()

 */

/* DELETE PSEUDOCODE
    two cases:
    1. htblColNameValue contains common columns with the index:
        use the columns to reach the suitable buckets
        1. find the list of the common columns
        2. iterate using each column on the grid to find the bucket(s) meeting the conditions
    2. Else, no common columns found ( how to search in the index ???????)

    R(id, name, age, major)
    index [ id, name]
    htbl{ id = 123,
        major = 'cs'}


   3D GRID [id, age, name] grid[][][] .. grid[:][pos]
   common{ age, name}
 */




/* Update Pseudocode

1-Since we store the primary key so we will check if it's indexed:
   a.if not then we use the old method
   b.if yes then we search for the row to be updated

2-Check which values are indexed:-
   a.if the values needed to be updated are not indexed then we update them in the table only
   b.if the values are indexed then we update them in table and in bucket

3-First update in the table
4-Then get the index of the new updated indexed values
5-Get the bucket they are stored in
6-update the values in the bucket and update the index



 */



/* Select PseudoCode


 */

