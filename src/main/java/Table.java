import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Vector;

public class Table implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String tableName, primaryKey, path;
	private transient Hashtable<String, String> htblColNameType, htblColNameMin, htblColNameMax, htblColNameIndexed;
	private int maxPageSize; 
	private int it = -1; //Iterator on pages
	private Hashtable<String, Integer> colOrder = new Hashtable<String, Integer>();
	private Vector<String> pageNames;

	private Vector<GridIndex> index; // a vector of all indices created on this table
	// SHOULD WE KEEP AN ARRAY OF PAGES (SIMILAR TO WHAT WE DID WITH PAGES AND Rows) ??!!!! NO

	public Table(String path,
				 String tableName,
				 String primaryKey,
				 int maxPageSize,
				 Hashtable<String, String> htblColNameType,
				 Hashtable<String, String> htblColNameMin,
				 Hashtable<String, String> htblColNameMax) throws DBAppException{
		
		// Initializing attributes
		this.path = path;
		this.tableName = tableName;
		this.primaryKey = primaryKey;
		this.maxPageSize = maxPageSize;
		this.pageNames = new Vector<String>();
		this.htblColNameType = htblColNameType;  
		this.htblColNameMin = htblColNameMin;	  
		this.htblColNameMax = htblColNameMax;  
		this.index = new Vector<GridIndex>();
//		readColConstraints();
		
		// Create directory of the table
		File dir = new File(path);
		dir.mkdirs();

//		//Create Page
//		createPage();
//		pageNames.add(tableName+"_"+"page"+it+".class");
		
		// Setting column order.. each column and its corresponding order in a
		// hashtable. This is useful going forward in insertion.
		initializeColOrder();
		
		// Saving the table in its directory
		save();
	}
	
	public int getIt() {
		return it;
	}

	public Vector<String> getPageNames() {
		return pageNames;
	}

	private void initializeColOrder(){
		int i = 0;
		for(Entry<String, String> entry : htblColNameType.entrySet()){
			String colName = entry.getKey();
			colOrder.put(colName, i);
			i++;
		}
	}
	
	// This method reads columns constraints from the metadata.csv file and 
	// stores it in the corresponding hashtables
	@SuppressWarnings("resource")
	private void readColConstraints() throws DBAppException{
		// load metadata.csv file
		String metadataPath = "src\\main\\resources\\metadata.csv";
		String line;
		BufferedReader br;

		this.htblColNameType = new Hashtable<String, String>();
		this.htblColNameMin = new Hashtable<String, String>();
		this.htblColNameMax = new Hashtable<String, String>(); 

		this.htblColNameIndexed = new Hashtable<String,String>();
		try {
			br = new BufferedReader(new FileReader(metadataPath));
			while( (line=br.readLine()) != null){
				String [] values = line.split(",");
				if( !(values[0].equals(tableName)) )
					continue;
				String colName = values[1];
				if(values[3] == "True")
					this.primaryKey = colName;
				this.htblColNameType.put(colName, values[2]);
				this.htblColNameMin.put(colName, values[5]);
				this.htblColNameMax.put(colName, values[6]);

				this.htblColNameIndexed.put(colName, values[4]);
			}
			br.close();
		} catch (FileNotFoundException e) {
			throw new DBAppException(e.getMessage());
		} catch (IOException e1) {
			throw new DBAppException(e1.getMessage());
		}
	}
	
	// creates a new page, and adds its name to the end of the vector
	private Page createPage() throws DBAppException {
		// TODO Auto-generated method stub
		Page p;
		int x = it+1;
		p = new Page(maxPageSize, path+tableName+"_"+"page"+x+".class");
		pageNames.add(x, tableName+"_"+"page"+x+".class");
		it++;
		save();
		return p;
	}
	
	// creates a new page, and adds its name to position=idx in the vector
	private Page createPage(int idx) throws DBAppException {
		// TODO Auto-generated method stub
		Page p;
		p = new Page(maxPageSize, path+tableName+"_"+"page"+it+".class");
		pageNames.add(idx, tableName+"_"+"page"+it+".class");
		it++;
		save();
		return p;
	}
	
	public Vector<Object> insert(Hashtable<String, Object> htblColNameValue) throws DBAppException{
		/*
		 * 1. check not null
		 * 2. check valid datatypes
		 * 3. Check min <= value <= max
		 * 4. check not duplicate clusteringKey
		 * 5. add record
		 */
		Vector<Object> rowPagePos = new Vector<Object>(); // vector of the 3 things to be returned.. useful for index insertion

		checkColumns(htblColNameValue);
		boolean pkFound = checkPrimaryKey(htblColNameValue);
		//if a duplicate primary key is found, don't insert
		if(pkFound){
			System.out.println("Primary key " + htblColNameValue.get(primaryKey).toString() +" already exists.");
			return null;
		}

		// Create a new Row instance for insertion
		Row r = makeNewRow(htblColNameValue);
		rowPagePos.add(r);

		//first, if table is empty, create a new page and insert directly.
		if(pageNames.size() == 0) {
			Page p = this.createPage();
			p.addRow(r);
			System.out.println("Insertion successful");
			rowPagePos.add(p);
			rowPagePos.add(0);
			return rowPagePos;
		}

		// get position of insertion (page index, position in page)
		Object pk = htblColNameValue.get(primaryKey);
		Vector<Integer> pos = getInsertionPos(htblColNameValue.get(primaryKey).toString());
//		System.out.println(htblColNameValue.get(primaryKey).toString());


		//if page pos = -1 then this means we are inserting value akbar mn ay value in the table
		// and it must be inserted at the end of the table.
		if(pos.get(0) == -1){
			Page last = getPage(pageNames.size()-1);
			//if last page is full, then create a new page and insert in it.
			// else, insert in the last page normally
			if(last.isFull()){
				Page p = createPage();
				p.addRow(r);
				rowPagePos.add(p);
				rowPagePos.add(0);
			}
			else {
				last.addRow(r);
				rowPagePos.add(last);
				rowPagePos.add(last.getRow().size()-1);
			}
			return rowPagePos;
		}
		// Read the current page
		Page curPage = getPage(pos.get(0));

		int lastRowIdx = curPage.size()-1;
		
		// if this page is not full, and new row is bigger than last row [pos=-1], insert in it
//		System.out.println(curPage.isFull());
		if(!curPage.isFull() ){
			if(pos.get(1) == -1){
				curPage.addRow(r);
				rowPagePos.add(curPage);
				rowPagePos.add(curPage.getRow().size()-1);
				return rowPagePos;
			}
			// YOU NEED TO SEE WHICH ROW IS BIGGER !!!!!!
			Row r1 = curPage.getRow(pos.get(1));
			int res = compareObjects(r, r1);
			if(res < 0) {
				curPage.addRow(r, pos.get(1));    // since we already compared pk values, insert directly
				rowPagePos.add(curPage);
				rowPagePos.add(pos.get(1));
			}
			else{
					curPage.addRow(r, pos.get(1) + 1);
					rowPagePos.add(curPage);
					rowPagePos.add(pos.get(1)+1);
				}
		}
		else{ 
			//if the current page is the last one, create a new page normally
			if(pos.get(0)+1 == pageNames.size()){
				Page newPage = createPage();
				
				//shift the last row of curPage to the newPage, then insert r in curPage
				newPage.addRow(curPage.getRow(lastRowIdx));
				curPage.deleteRow(lastRowIdx);
				
				// YOU NEED TO SEE WHICH ROW IS BIGGER !!!!!!
				//Row r1 is the row at position of insertion
				Row r1 = curPage.getRow(pos.get(1));
				int res = compareObjects(r, r1);
				rowPagePos.add(curPage);
				if(res < 0) {
					curPage.addRow(r, pos.get(1));
					rowPagePos.add(pos.get(1));
				}
				else{
						curPage.addRow(r, pos.get(1) + 1);
						rowPagePos.add(pos.get(1)+1);
					}
			}
			else{
				// Else check the next page. If it's not full, insert. Else, create a new page with special procedures
				
				// First we have to open the next page.
				Page nextPage = getPage(pos.get(0)+1);
				
				//if the next page is not full, shift to its top the last row, then insert in curPage
				if( !nextPage.isFull() ){
					// if I want to insert in the last row, compare first then insert
					rowPagePos.add(nextPage);
					if(pos.get(1) == lastRowIdx){
						Row r1 = curPage.getRow(pos.get(1));
						int res = compareObjects(r, r1);
						if(res < 0){
							nextPage.addRow(r1,0);
							curPage.deleteRow(lastRowIdx);
							curPage.addRow(r);
							rowPagePos.add(lastRowIdx);
						}
						else{
							// this else part should never be reached because if the
							// last row is smaller than insertion row, this page should
							// not be selected for insertion.
							nextPage.addRow(r, 0);
							rowPagePos.add(0);
						}
						return rowPagePos;
					}
					Row lastRow = curPage.getRow(lastRowIdx);
					nextPage.addRow(lastRow, 0);
					curPage.deleteRow(lastRowIdx);
					
					// YOU NEED TO SEE WHICH ROW IS BIGGER !!!!!!
					Row r1 = curPage.getRow(pos.get(1));
					int res = compareObjects(r, r1);
					if(res < 0) {
						curPage.addRow(r, pos.get(1));
						rowPagePos.add(pos.get(1));
					}
					else {
						curPage.addRow(r, pos.get(1) + 1);
						rowPagePos.add(pos.get(1)+1);
					}
				}
				else{
					// if the next page is also full, create a new page and shift to it the last
					// row in curPage, then insert in curPage. The new page will be put in the right order.
					Page newPage = createPage(pos.get(0)+1);
					if(pos.get(1) == lastRowIdx){
						Row r1 = curPage.getRow(pos.get(1));
						int res = compareObjects(r, r1);
						if(res < 0){
							newPage.addRow(r1,0);
							curPage.deleteRow(lastRowIdx);
							curPage.addRow(r);

							rowPagePos.add(curPage);
							rowPagePos.add(lastRowIdx);
						}
						else{
							// this else part should never be reached because if the
							// last row is smaller than insertion row, this page should
							// not be selected for insertion.
							newPage.addRow(r, 0);
							rowPagePos.add(newPage);
							rowPagePos.add(0);
						}
						return rowPagePos;
					}
					Row lastRow = curPage.getRow(lastRowIdx);
					newPage.addRow(lastRow);
					curPage.deleteRow(lastRowIdx);
					
					Row r1 = curPage.getRow(pos.get(1));
					int res = compareObjects(r, r1);
					rowPagePos.add(curPage);
					if(res < 0) {
						curPage.addRow(r, pos.get(1));
						rowPagePos.add(pos.get(1));
					}
					else {
						curPage.addRow(r, pos.get(1) + 1);
						rowPagePos.add(pos.get(1)+1);
					}
				}
			}
		}
		this.save();
		System.out.println("Insertion successful.");
		return rowPagePos;
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

	// compares two rows according to their primary keys
	private int compareObjects(Object r1, Object r2) throws DBAppException{
		int pkidx = colOrder.get(primaryKey);
		Object obj1 = ((Row) r1).getValue(pkidx);
		Object obj2 = ((Row) r2).getValue(pkidx);
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
	
	private Vector<Integer> getInsertionPos(String pkValue) throws DBAppException{
		int n = pageNames.size();
		Vector<Integer> x = new Vector<Integer>();
		for(int i = 0; i < n; i++){
			Page curPage = getPage(i);
			int pos = binarySearch(curPage.getRow(), pkValue);
			// if lastRow's pk is smaller than pkValue, pos = -1
			if(pos == -1)
				continue;
			x.add(i);
			x.add(pos);
			return x;
		}
		// if pkValue is larger than all other PKs, return (-1,-1)
		x.add(-1);
		x.add(-1);
		return x;
	}
	
	// determining type of the primary key, then
	// doing binary search on primary key of type String
	private int binarySearch(Vector<Row> rows, String searchVal) throws DBAppException{
		readColConstraints();
		//getting column index of the primary key
		int pkIdx = colOrder.get(primaryKey);
		int n = rows.size();
		String pkType = htblColNameType.get(primaryKey);
		if(pkType.equals("java.lang.Integer"))
			return bs(rows, Integer.parseInt(searchVal)); 
		else if(pkType.equals("java.lang.Double"))
			return bs(rows, Double.parseDouble(searchVal));
		else if(pkType.equals("java.util.Date")){
			Date d;
			try {
//				System.out.println(searchVal.toString());
				String newDate = convertDateFormat(searchVal);
				d = (Date) (new SimpleDateFormat("yyyy-MM-dd").parse(newDate));
			} catch (ParseException e) {
				throw new DBAppException(e.getMessage());
			}

			return bs(rows, d);
		}
		else{
			//if last row is smaller than the search value, return -1 (not found)
			if(n==0){
				return -1;
			}
			String lastRow = (String) rows.get(n-1).getValue(pkIdx);
			if( lastRow.compareTo(searchVal) < 0){
				return -1;
			}


//			System.out.println("inserting: " + searchVal);
			int start = 0, end = n-1;
			int mid = (start+end)/2;
//			System.out.println(start + " " + mid + " " + end);
			while(start <= end){
				// if start == end, then we have narrowed down our search to just one position
				if(start == end)
					return start;
				
				String midRow = (String) rows.get(mid).getValue(pkIdx);
				int res = midRow.compareTo(searchVal);

				if(res < 0)
					start = mid+1;
				else
					end = mid-1;
				mid = (start+end)/2;

//				System.out.println(res + " " + midRow);
//				System.out.println(start + " " + mid + " " + end);
			}
			// this return value should be reached if insertion is smaller than all values in the page
//			System.out.println("returning 0 from bs");
			return end+1;
		}
	}
	
	// doing binary search on primary key of type Date
	private int bs(Vector<Row> rows, Date searchVal) throws DBAppException {
		int n = rows.size();
		int pkIdx = colOrder.get(primaryKey);
		Date lastRow = new Date();
		try {
			if(n==0){
				return -1;
			}
			String newDate = convertDateFormat((rows.get(n-1).getValue(pkIdx)).toString());
			lastRow = new SimpleDateFormat("yyyy-MM-dd").parse(newDate);
		} catch (ParseException e) {
			throw new DBAppException(e.getMessage());
		}

		if( lastRow.compareTo(searchVal) < 0)
			return -1;
		
		int start = 0, end = n-1;
		int mid = (start+end)/2;
		while(start <= end){
			// if start == end, then we have narrowed down our search to just one position
			if(start == end)
				return start;
			
			Date midRow = new Date();
			try {
				String newDate = convertDateFormat(rows.get(mid).getValue(pkIdx).toString());
				midRow = new SimpleDateFormat("yyyy-MM-dd").parse( newDate );
			} catch (ParseException e) {
				throw new DBAppException(e.getMessage());
			}
			if(midRow.compareTo(searchVal) < 0)
				start = mid+1;
			else
				end = mid-1;
			mid = (start+end)/2;
		}
		return end+1;
	}
	
	// doing binary search on primary key of type double
	private int bs(Vector<Row> rows, double searchVal) {
		int n = rows.size();
		int pkIdx = colOrder.get(primaryKey);
		double lastRow;
		if(n==0)
			return -1;
		else
			lastRow = Double.parseDouble(rows.get(n-1).getValue(pkIdx).toString());
		if( lastRow < searchVal)
			return -1;
		
		int start = 0, end = n-1;
		int mid = (start+end)/2;
		while(start <= end){
			// if start == end, then we have narrowed down our search to just one position
			if(start == end)
				return start;
			
			double midRow = Double.parseDouble( rows.get(mid).getValue(pkIdx).toString());
			if(midRow < searchVal)
				start = mid+1;
			else
				end = mid-1;
			mid = (start+end)/2;
		}
		return end+1;
	}

	// doing binary search on primary key of type int
	private int bs(Vector<Row> rows, int searchVal) {
		int n = rows.size();
		int pkIdx = colOrder.get(primaryKey);
		int lastRow;
		if(n==0)
			return -1;
		else
			lastRow = Integer.parseInt(rows.get(n-1).getValue(pkIdx).toString());
		if( lastRow < searchVal)
			return -1;
		
		int start = 0, end = n-1;
		int mid = (start+end)/2;
		while(start <= end){
			// if start == end, then we have narrowed down our search to just one position
			if(start == end)
				return start;
			
			int midRow = Integer.parseInt( rows.get(mid).getValue(pkIdx).toString());
			if(midRow < searchVal)
				start = mid+1;
			else
				end = mid-1;
			mid = (start+end)/2;
		}
		return end+1;
	}

	// construct the new row to prepare it for insertion in the page.
	private Row makeNewRow(Hashtable<String, Object> htblColNameValue){
		int numCols = htblColNameMin.size();
		Row r = new Row(numCols);
		for(Entry<String, Object> entry : htblColNameValue.entrySet()){
			String key = entry.getKey();
			Object val = entry.getValue();
			// get index (order of this column)
			//					initializeColOrder();
			//					System.out.println(colOrder.size());

			int idx = colOrder.get(key);
			r.addValue(idx, val);
		}
		return r;
	}

	/* 
	 * 1. reads column constraints and datatypes
	 * 2. check each value meets those constrain
	 */
	private void checkColumns(Hashtable<String, Object> htblColNameValue) throws DBAppException{
		// making sure columns' constraints are read properly
		if(htblColNameMax == null || htblColNameMin == null || htblColNameType == null)
			readColConstraints();
		
		// checking every value for the correct constraints
		for(Entry<String, Object> entry : htblColNameValue.entrySet()){
			String key = entry.getKey();
			//1. check column exists
			if( !htblColNameValue.containsKey(key) )
				throw new DBAppException("Column " + key + " does not exist in table " + tableName);
			
			//2. check column type matches
			Object val = entry.getValue(); 
			String existingType = htblColNameType.get(key)+"";
			if( !(checkColType(val , existingType)))
				throw new DBAppException("Entered column type " + val.getClass().getSimpleName() + 
						" mismatches required type " + existingType);
			
			// 3. check min <= value <= max
			Object min = htblColNameMin.get(key)+"";
			Object max = htblColNameMax.get(key)+"";
			if(val instanceof Integer){
				if( !(((Integer) val).compareTo(Integer.parseInt((String) min)) >= 0 
						&& ((Integer) val).compareTo(Integer.parseInt((String) max)) <= 0)) 
					throw new DBAppException("Value " + val.toString() + " in column " + key + " must be between " + min.toString()
							+ " and " + max.toString());
			}
			if(val instanceof String){
				if( !(((String) val).compareTo((String) min) >= 0
						&& ((String) val).compareTo((String) max) <= 0)) 
					throw new DBAppException("Value " + val.toString() + " in column " + key + " must be between " + min.toString()
							+ " and " + max.toString());
			}
			if(val instanceof Date){
				if( (val+"").compareTo((String)min) >= 0 && (val+"").compareTo((String) max) <= 0) 
					throw new DBAppException("Value " + val.toString() + " in column " + key + " must be between " + min.toString()
							+ " and " + max.toString());
			}
			if(val instanceof Double){
				if( !(((Double) val).compareTo(Double.parseDouble((String) min)) >= 0 
						&& ((Double) val).compareTo(Double.parseDouble((String) max)) <= 0)) 
					throw new DBAppException("Value " + val.toString() + " in column " + key + " must be between " + min.toString()
							+ " and " + max.toString());
			}
		}
	}
	
	//checks if value has the same datatype as type
	private boolean checkColType(Object value, String type) {
		if(type.equals("java.lang.Integer")) {
			if (!(value instanceof Integer))
				return false;
		}
		if(type.equals("java.lang.String")) {
			if (!(value instanceof String))
				return false;
		}
		if(type.equals("java.lang.Double")) {
			if (!(value instanceof Double))
				return false;
		}
		if(type.equals("java.util.Date")) {
			if (!(value instanceof Date))
				return false;
		}
		return true;
	}
	
	private boolean checkPrimaryKey(Hashtable<String,Object> htblColNameValue) throws DBAppException{
		// check if primary key is missing
		if(!(htblColNameValue.containsKey(primaryKey)))
			throw new DBAppException("Primary key is missing.");
		
		//check if primary key value is null
		Object primaryVal = htblColNameValue.get(primaryKey);
		if(primaryVal == null)
			throw new DBAppException("Primary keys can't be null");

		Row r1 = makeNewRow(htblColNameValue);

		// search for this primary key in the table. If found, return true
		int n = pageNames.size();
		for(int i = 0; i < n; i++){
			Page p = getPage(i);
			int pos = binarySearch(p.getRow(), primaryVal.toString());
			// if a duplicate primary key is found, return true
			if(pos == -1 || pos >= p.getRow().size())
				continue;
			Row r2 = p.getRow(pos);
			int x = compareObjects(r1,r2);
			if(x==0)
				return true;
		}
		return false;
	}

	private void save() throws DBAppException {
		// TODO Auto-generated method stub
		File f = new File(path+tableName+".class");
		
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

	public void update(String clusteringKeyValue,
			Hashtable<String, Object> columnNameValue) throws DBAppException {
		// check if all constraints are met
		checkColumns(columnNameValue);
		
		// if primary key is attempted to be edited, throw an exception.
		if(columnNameValue.containsKey(clusteringKeyValue))
			throw new DBAppException("Editing of primary key is prohibited!");
		
		boolean rowFound = false; 		// boolean flag to indicate whether the row was found
		
		// 1. find the row to be updated by using binary search on every page.
		for(int i = 0; i <= it; i++){
			Page curPage = getPage(i);
			int pos = binarySearch(curPage.getRow(), clusteringKeyValue);
			if(pos == -1)
				continue;
			else{
				rowFound = true;
				
				// 3. update the row to the new column values. Unmentioned columns should stay the same
				for(Entry<String, Object> entry:columnNameValue.entrySet())
				{
					String colName = entry.getKey();
					Object colValue = entry.getValue();
					
					int colIdx = colOrder.get(colName);
					Row updatedRow = curPage.getRow(pos);
					updatedRow.addValue(colIdx, colValue);
				}
				
				// 4. save
				this.save();
				break;
			}
		}
		
		if(!rowFound){
			//throw new DBAppException("Row of clustering key = '" + clusteringKeyValue + "' is not found.");
			System.out.println("Row of clustering key = '" + clusteringKeyValue + "' is not found.");
			return;
		}

//		System.out.println("Row updated successfully!");
	}
	
	// This method loads and returns page of pos=idx
	public Page getPage(int idx) throws DBAppException{
		String curPageName = pageNames.get(idx);
		File f = new File(path+curPageName);
		Page curPage = null;
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(new FileInputStream(f));
			curPage = (Page) ois.readObject();
			ois.close();
			return curPage;
		} catch (FileNotFoundException e) {
			throw new DBAppException(e.getMessage());
		} catch (IOException e) {
			throw new DBAppException(e.getMessage());
		}catch (ClassNotFoundException e) {
			throw new DBAppException(e.getMessage());
		}
	}
	
	/**
	 * This method takes a hashtable with conditions of the deletion
	 * and searches for rows satisfying all conditions then deletes them.
	 * If any row is found with those conditions, it returns true, else false.
	 */
	public boolean delete(Hashtable<String, Object> columnNameValue) throws DBAppException{
		
		boolean found = false; // result of deletion
		
		//If the conditions include a primary key, we will search for this row using 
		//binary search in a separate method.
		if(columnNameValue.containsKey(primaryKey))
			found = deleteWithPk(columnNameValue);
		//Else, we will load each page and search linearly for the
		//rows satisfying the conditions
		else{
			// looping from last to first page to avoid problems in indexing after deletion
			int n = pageNames.size();
			for(int i = n-1; i >= 0; i--){
				Page curPage = getPage(i);
				int n1 = curPage.size();
				// looping from last to first row to avoid problems in indexing after deletion
				for(int j = n1-1; j >= 0; j--){
					Row curRow = curPage.getRow(j); 	// current row in the page
					
					boolean satisfies = true; 	// boolean flag to know if the row meets the conditions.
					
					for(Entry<String, Object> entry : columnNameValue.entrySet()){
						int colIdx = colOrder.get(entry.getKey());
						if(!curRow.getValue(colIdx).equals(entry.getValue())){
							satisfies = false; 	// if a condition is not met, this row doesn't satisfy.
							break;
						}
					}
					// if row satisfies conditions, delete it.
					if(satisfies){
						curPage.deleteRow(j);
						found = true;
					}
				}
				// if page is now empty after deletion, delete it too.
				if(curPage.size() == 0)
					deletePage(i);
			}
		}
		this.save();
		return found;
	}

	/**
	 *	This method is used to execute delete queries including condition on primary key
	 *	column. It uses binary search on each page to find the row to be deleted. 
	 */
	private boolean deleteWithPk(Hashtable<String, Object> columnNameValue) throws DBAppException {
		boolean found = false;
		for(int i = it; i >= 0; i--){
			Page curPage = getPage(i);
			String pkCondition = columnNameValue.get(primaryKey).toString();
//			System.out.println(pkCondition);
			// getting position of the row using binary search. if not found, returns -1
			int pos = binarySearch(curPage.getRow(), pkCondition);
			
			// if key is not found in this page, move on to the next page
			if(pos == -1)
				continue;
			
			// else if key is found, proceed to check the rest of the conditions
			Row curRow = curPage.getRow(pos); 	// get the row from the page
			
			boolean satisfies = true; 	// boolean flag to know if the row meets all the conditions.
			
			for(Entry<String, Object> entry : columnNameValue.entrySet()){
				int colIdx = colOrder.get(entry.getKey());
				if(!curRow.getValue(colIdx).equals(entry.getValue())){
					satisfies = false; 	// if a condition is not met, this row doesn't satisfy.
					break;
				}
			}
			// if row satisfies conditions, delete it.
			if(satisfies){
				curPage.deleteRow(pos);
				found = true;
				// if current page became empty, delete it too.
				if(curPage.size() == 0)
					deletePage(i);
				break;
			}
		}
		return found;
	}

	/**
	 * This methods deletes a page from the disk.
	 * @param i : index of the page name in vector pageNames
	 */
	private void deletePage(int i) throws DBAppException {
		String page = pageNames.get(i);
		File f = new File(path + page);
		f.delete();
		pageNames.remove(i);
		it--;
		this.save();
	}

	public void addIndex(GridIndex gi) throws DBAppException {
		index.add(gi);
		save();
	}

	public Vector<GridIndex> getIndex() {
		return index;
	}

	public Hashtable<String, String> getHtblColNameType() {
		return htblColNameType;
	}

	public Hashtable<String, String> getHtblColNameMax() {
		return htblColNameMax;
	}

	/**
	 * this method inserts the given row in all indexes created in this table.
	 * @param rowPagePos a vector that contains [row to be inserted, page that contains the row, position of row in its page]
	 */
	public void insertIntoIndexes(Vector<Object> rowPagePos) {
		for(GridIndex gi : index){
			gi.insert(rowPagePos, htblColNameType);
		}
	}
}
