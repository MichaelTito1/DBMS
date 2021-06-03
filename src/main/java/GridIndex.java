import java.io.*;
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
    private Vector<Bucket> grid;
    private Vector<String> indexedCols; // vector names of indexed columns
    private Vector<String> colTypes;    // vector of datatypes of indexed columns
    private Vector<Object> colMin;      // vector of minimum size of each indexed column IN ORDER
    private Vector<Object> colMax;      // vector of maximum size of each indexed column IN ORDER
    // key = column name, Value: a vector of size 10, each element contains its upper boundary. It should respect column datatypes.
    private Hashtable<String, Vector<Object>> htblRanges;
    private boolean pkIndexed = false;  // a flag to indicate if this index is created on the primary key
    private int pkPos;
    private Table parentTable;

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
    public GridIndex(Table t,
                     String path,
                     String tableName,
                     String[] colNames,
                     int maxRowsPerBucket,
                     Hashtable<String,String> htblColTypes,
                     Hashtable<String, String> htblColMin,
                     Hashtable<String,String> htblColMax,
                     int pkPos) throws DBAppException {
        this.parentTable = t;
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
            htblRanges.put(col, new Vector<Object>(11));

        // dynamically creating a grid
//        final int[] dimensions = new int[colNames.length];
//        Arrays.fill(dimensions, 10);
//        grid = (Object[]) Array.newInstance(Object.class, dimensions);
        this.grid = new Vector<Bucket>((int)Math.pow(11, colNames.length));

        // divide each dimension into 11 divisions
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
     * Takes n-coordinates and converts them to a single number
     * corresponding to the position in the 1D grid.
     * @param coordinates
     * @return
     */
    private int toPosition(Vector<Integer> coordinates){
        int ans = 0;
        int n = coordinates.size();
        for(int i = 0; i < n; i ++){
            int dim = coordinates.get(i);
            ans += dim * Math.pow(11, i);
        }
        return ans;
    }

    /**
     * This method takes a single number and converts it to the corresponding
     * coordinates with the suitable number of dimensions.
     * @param pos
     * @return
     */
    private Vector<Integer> toCoordinates(int pos){
        Vector<Integer> ans = new Vector<Integer>();
        int x = (pos%11);
        ans.add(x);
        int newPos = pos - x;
        int power = 1;
        while(newPos > 0){
            int newDim =  ( newPos/(int)Math.pow(11, power) ) % 11;
            newPos -= newDim*Math.pow(11, power);
            power++;
            ans.add(newDim);
        }
        return ans;
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
                stringDivisions(i);
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
        int delta = Integer.parseInt( (String) colMax.get(i))/10; // delta is how big each unit of divisions is (maximumValue/10)
        Vector<Object> range = htblRanges.get(colName); // vector that holds the ranges
        for(int j = 0; j < 10; j++)
            range.set(j, (j+1)*delta); // upper bound of this cell = (j+1)*(maximumValue/10)
        range.set(10, null);
    }

    private void doubleDivisions(int i){
        String colName = indexedCols.get(i);             // column name
        double delta = Double.parseDouble( (String) colMax.get(i))/10.0; // delta is how big each unit of divisions is (maximumValue/10)
        Vector<Object> range = htblRanges.get(colName); // vector that holds the ranges
        for(int j = 0; j < 10; j++)
            range.set(j, ((double)j+1.0) *delta); // upper bound of this cell = (j+1)*(maximumValue/10)
        range.set(10, null);
    }

    /**
     * This method divides string into ranges according to their length
     * @param i index of the column
     */
    private void stringDivisions(int i){
        String colName = indexedCols.get(i);
        String mini = (String) colMin.get(i);
        String maxi = (String) colMax.get(i);

        int minLen = mini.length();
        int maxLen = maxi.length();

        int delta = (int) Math.ceil((maxLen-minLen)/10);
        Vector<Object> range = htblRanges.get(colName);
        for(int j = 0; j < 10; j++)
            range.set(j, (j+1)*delta);
        range.set(10,null);
    }

    /**
     * divides date into ranges according to the difference between max
     * and min dates in milliseconds.
     * @param i
     */
    private void dateDivisions(int i) {
        String colName = indexedCols.get(i);             // column name
        Vector<Object> range = htblRanges.get(colName); // vector that holds the ranges

        Date mini = (Date) colMin.get(i);
        Date maxi = (Date) colMax.get(i);
        Long delta = (maxi.getTime() - mini.getTime())/10; // get difference between max and min in milliseconds
        for(int j = 0; j < 10; j++)
            range.set(j, (j+1)*delta); // upper bound of this cell = (j+1)*(maximumValue/10)
        range.set(10, null);
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

    /**
     * This method is used to insert the row in the correct position in the index
     * @param rowPagePos
     */
    public void insert(Vector<Object> rowPagePos, Hashtable<String,String> htbl) throws DBAppException {
        // does some wizardry to find the suitable bucket
        Row r = (Row) rowPagePos.get(0);
        Vector<Integer> filtered = filterIndexedCols(htbl, r);
        Vector<Integer> gridPos = new Vector<Integer>();
        Vector<Object> relevantCols = new Vector<Object>();
        for(Integer pos : filtered){
            String colName = r.getColNames().get(pos);
            Vector<Object> range = htblRanges.get(colName); // max age 30 : [3,6,9,12,...,30] insert 9
            int n = range.size();
            Object colVal = r.getValue(pos);
            relevantCols.add(colVal);
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

        // create the reference, then calculate its final position in the grid vector.
        Reference ref = new Reference( ((Page)rowPagePos.get(1)).getPath(), r.getValue(pkPos), relevantCols);
        int finalPos = toPosition(gridPos);

        // If grid cell is empty, create new bucket. Else, insert in the existing bucket
        if(grid.get(finalPos) == null) {
            Bucket b = createBucket(colMin,colMax);
            grid.set(finalPos,b);
        }
        ( (Bucket)grid.get(finalPos)).insert(ref);
        save();
    }

    /**
     * This gets the target cell on which we should do an operation [insert/update/delete/select]
     * @param r
     * @param
     * @return
     * @throws DBAppException
     */
//    private Bucket getTargetCell(Row r, Vector<Integer> filtered,) throws DBAppException {
//        Vector<Integer> gridPos = new Vector<Integer>();
//
//        for(Integer pos : filtered){
//            String colName = r.getColNames().get(pos);
//            Vector<Object> range = htblRanges.get(colName); // max age 30 : [3,6,9,12,...,30] insert 9
//            int n = range.size();
//            Object colVal = r.getValue(pos);
//
//            for(int i = 0; i < n; i++){
//                Object div = range.get(i);
//                int res = compareObjects(div, colVal);
//                if(res > 0){
//                    // if division upper boundary is greater than the column value, save division's
//                    // position and exit the loop.
//                    gridPos.add(i);
//                    break;
//                }
//            }
//        }
//
//        // narrowing down on dimensions until we reach the last dimension which will give
//        // us the bucket in which we should insert
//        int n = gridPos.size();
//        Object[] x = (Object[]) grid[gridPos.get(0)];
//        for(int i = 1; i < n-1; i++)
//            x = (Object[]) x[gridPos.get(i)];
//
//        Bucket targetBucket = (Bucket) x[gridPos.get(n-1)];
//        if(targetBucket == null)
//            targetBucket = createBucket(colMin, colMax);
//        return targetBucket;
//    }

    public Vector<Integer> filterIndexedCols(Hashtable<String, String> htbl, Row r){
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
     * This performs SHALLOW delete on a given row USING BRUTE FORCE. it is deleted only from the index.
     */
    public boolean delete(Hashtable<String, Object> columnNameValue) throws DBAppException {
        boolean done = true;
        //for each bucket and its overflow:
        // find satisfying references and delete their rows then delete the references
        for(Bucket b : grid){
            Vector<Reference> refs = b.getReferences();
            boolean satisfies = false;
            for(Reference ref : refs){
                satisfies = checkReference(ref, columnNameValue);
                if(satisfies){
                    // delete from bucket
                    done = done && b.deleteReference(refs.indexOf(ref));
                }
            }
            // if no reference in this bucket satisfied the conditions, check its overflow bucket.
            if(!satisfies)
                done = done && shallowDeleteFromOverflow(b.getNextBucket(), columnNameValue);
        }
        return done;
    }

    /**
     * given a bucket, try to deep delete a reference. If it fails, execute on the next bucket recursively
     * @param b
     * @param columnNameValue
     * @return
     */
    private boolean shallowDeleteFromOverflow(Bucket b, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if(b == null)
            return false;
        boolean done = true;
        Vector<Reference> refs = b.getReferences();
        boolean satisfies = false;
        for(Reference ref : refs){
            satisfies = checkReference(ref, columnNameValue);
            if(satisfies){
                // delete from bucket
                done = done && b.deleteReference(refs.indexOf(ref));
            }
        }
        if(!satisfies) // if reference is not found here, search in the next overflow.
            done = done && shallowDeleteFromOverflow(b.getNextBucket(), columnNameValue);
        return done;
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

    public String getPath() {
        return path;
    }

    public Vector<String> getIndexedCols() {
        return indexedCols;
    }

    /**
     * this method deletes the rows satisfying the given conditions from both
     * the table and the index, hence the name deepDelete.
     * @param columnNameValue
     * @param indexedCols
     * @return
     */
    public boolean deepDelete(Hashtable<String, Object> columnNameValue, Vector<String> indexedCols) throws DBAppException {
        boolean done = true;
        // 1. todo : locate buckets satisfying the conditions
        Vector<Bucket> buckets = ;
        // 2. : for each bucket and its overflow:
        // 2a. : find satisfying references and delete their rows then delete the references
        for(Bucket b : buckets){
            Vector<Reference> refs = b.getReferences();
            boolean satisfies = false;
            for(Reference ref : refs){
                satisfies = checkReference(ref, columnNameValue);
                if(satisfies){
                    // delete from table
                    parentTable.deleteRow(ref.getPageAddress(), ref.getPrimaryKey());
                    // delete from bucket
                    done = done && b.deleteReference(refs.indexOf(ref));
                }
            }
            // if no reference in this bucket satisfied the conditions, check its overflow bucket.
            if(!satisfies)
                done = done && deepDeleteFromOverflow(b.getNextBucket(), columnNameValue);
        }
        return done;
    }

    /**
     * given a bucket, try to deep delete a reference. If it fails, execute on the next bucket recursively
     * @param b
     * @param columnNameValue
     */
    private boolean deepDeleteFromOverflow(Bucket b, Hashtable<String, Object> columnNameValue) throws DBAppException {
        if(b == null)
            return false;
        boolean done = true;
        Vector<Reference> refs = b.getReferences();
        boolean satisfies = false;
        for(Reference ref : refs){
            satisfies = checkReference(ref, columnNameValue);
            if(satisfies){
                // delete from table
                parentTable.deleteRow(ref.getPageAddress(), ref.getPrimaryKey());
                // delete from bucket
                done = done && b.deleteReference(refs.indexOf(ref));
            }
        }
        if(!satisfies) // if reference is not found here, search in the next overflow.
            done = done && deepDeleteFromOverflow(b.getNextBucket(), columnNameValue);
        return done;
    }

    /**
     * checks if a reference satisfies conditions in the hashtable.
     * @param ref
     * @param columnNameValue
     * @return
     */
    private boolean checkReference(Reference ref, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // get row from the table using primary key
        int pagePos = parentTable.getPageNames().indexOf(ref.getPageAddress());
        Page p = parentTable.getPage(pagePos); // load page
        // get row position in the page, then get row itself
        int rowPos = parentTable.binarySearch(p.getRow(), ref.getPrimaryKey().toString());
        Row r = p.getRow(rowPos);

        // check if conditions are met, then delete.
        for(Map.Entry<String, Object> entry : columnNameValue.entrySet()){
            int x = r.getColNames().indexOf(entry.getKey());
            int comp = compareObjects(r.getValue(x), entry.getValue());
            if(comp != 0) // if any condition is not satisfied, reject the row.
                return false;
        }
        return true;
    }

    /**
     * deletes given rows using brute force. this method is used only when
     * @param toBeDeleted
     */
    public void deleteBruteForce(Vector<Row> toBeDeleted) throws DBAppException {

        for(Row del : toBeDeleted) {
            Object delPk = del.getValue(pkPos);
            for (Bucket b : grid) {
                b.deleteRow(delPk);
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

