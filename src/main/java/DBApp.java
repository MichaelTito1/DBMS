import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

public class DBApp implements DBAppInterface {

	private File metadata;
	private Properties prop;
	//private String mainDir = "DB_app/";  //WILL THE SPACE CAUSE ERRORS?!!!!!
	private String mainDir = "src\\main\\resources\\data\\";
	private Vector<String> datatypes = new Vector<String>();

	@Override
	public void init() {

		//		//Check if this dbName already exists. If it already exists, notify the user.
		//		File f = new File(mainDir + this.dbName + "/");
		//		if(f.exists()){
		//			throw new DBAppException();
		//		}
		//		this.dbName = dbName;
		//		
		//		//Create directory for the new DB
		//		f.mkdir();
		//		dbDirectory = mainDir + this.dbName + "/";
		//		
		// Getting the config file
		try {
			fetchConfigFile();
		} catch (DBAppException e) {
			System.out.print(e.getMessage());
		}


		// Getting the metadata file
		this.metadata = new File("src\\main\\resources\\metadata.csv");

		// make a new directory for the tables
		File f = new File(mainDir);
		f.mkdirs();

		// add supported column datatypes
		datatypes.add("java.lang.Integer");
		datatypes.add("java.lang.String");
		datatypes.add("java.lang.Double");
		datatypes.add("java.util.Date");

	}

	@Override
	public void createTable(String tableName, String strClusteringKeyColumn,
							Hashtable<String, String> htblColNameType,
							Hashtable<String, String> htblColNameMin,
							Hashtable<String, String> htblColNameMax) throws DBAppException {

		// Check if this table name already exists
		File f = new File(mainDir + tableName + ".class");
		if (f.exists())
			throw new DBAppException("Table " + tableName + " already exists. Please choose a different name.");

		// check if all column types are supported
		checkColTypes(htblColNameType);

		//		// add table info to the metadata.csv file
		//		addTableToMeta(tableName, htblColNameType, htblColNameMin, htblColNameMax, strClusteringKeyColumn);


		// Create the new table 
		int maxPageSize = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		//		System.out.println(maxPageSize);
		new Table(mainDir, tableName, strClusteringKeyColumn, maxPageSize, htblColNameType, htblColNameMin, htblColNameMax);

		// add table info to the metadata.csv file
		addTableToMeta(tableName, htblColNameType, htblColNameMin, htblColNameMax, strClusteringKeyColumn);

		System.out.println("Table " + tableName + " is created successfully");
	}

	@Override
	public void createIndex(String tableName, String[] columnNames)
			throws DBAppException {
		// TODO Auto-generated method stub
		// load table
		Table t = getTable(tableName);

		// create the new index on the requested columns
        int maxBucketSize = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
        GridIndex gi = new GridIndex(mainDir, tableName, columnNames, maxBucketSize);

        // add the index to the table
        t.addIndex(gi);
	}

	@Override
	public void insertIntoTable(String tableName,
								Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		Table t;
		t = getTable(tableName);

		if (t == null)
			throw new DBAppException("Table " + tableName + " not found!");

		t.insert(colNameValue);
	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue,
							Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		Table t;
		t = getTable(tableName);
		if (t == null)
			throw new DBAppException("Table " + tableName + " not found!");
		t.update(clusteringKeyValue, columnNameValue);
		System.out.println("Updating row of primary key '" + clusteringKeyValue + "' is successful.");
	}

	@Override
	public void deleteFromTable(String tableName,
								Hashtable<String, Object> columnNameValue) throws DBAppException {

		Table t = getTable(tableName);
		if (t == null)
			throw new DBAppException("Table " + tableName + " not found!");

		boolean done = t.delete(columnNameValue);
		if (done) {
			System.out.println("Delete query successful.");
		} else {
			//throw new DBAppException("Deletion failed. Rows satisfying all query conditions were not found.");
			System.out.println("Deletion failed. Rows satisfying all query conditions were not found.");
		}

	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators)
			throws DBAppException {
		// TODO Auto-generated method stub
		return null;
	}


	/* TO BE TESTED
	 * This function adds the metadata of the given table
	 * to the metadata file
	 */
	private void addTableToMeta(String tableName,
								Hashtable<String, String> colNameType,
								Hashtable<String, String> colNameMin,
								Hashtable<String, String> colNameMax,
								String clusteringKey) throws DBAppException {
		PrintWriter pr;
		try {
			pr = new PrintWriter(new FileWriter(metadata, true));
		} catch (IOException e) {
			throw new DBAppException(e.getMessage());
		}

		for (Entry<String, String> entry : colNameType.entrySet()) {
			String colName = entry.getKey();
			String colType = entry.getValue();
			boolean isPrimary = colName.equals(clusteringKey);
			pr.append(tableName + "," + colName + "," + colType + "," + isPrimary + "," + "false" + "," + colNameMin.get(colName) + "," + colNameMax.get(colName) + "," + "\n");
		}
		pr.flush();
		pr.close();
	}

	private void fetchConfigFile() throws DBAppException {

		this.prop = new Properties();
		String fileName = "src\\main\\resources\\DBApp.config";
		InputStream is;
		try {
			is = new FileInputStream(fileName);
		} catch (FileNotFoundException e) {
			throw new DBAppException(e.getMessage());
		}

		try {
			this.prop.load(is);
		} catch (IOException e) {
			throw new DBAppException(e.getMessage());
		}

		//		String fileName = "DBApp.config";
		//		
		//		String p= "";
		//		InputStream is = null;
		//		is = new FileInputStream(p + fileName);
		//		
		//		prop.load(is);
		//		
		//		 assigning the max page size from the configuration file
		//		this.maxPageSize = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		//		System.out.println(maxPageSize);

	}

	private void checkColTypes(Hashtable<String, String> htblColNameType) throws DBAppException {
		for (Entry<String, String> e : htblColNameType.entrySet()) {
			if (!datatypes.contains(e.getValue()))
				throw new DBAppException("Invalid Column: " + e.getKey() + " Datatype : " + e.getValue());
		}
	}

	public Table getTable(String tableName) throws DBAppException {
		File f = new File(mainDir + tableName + ".class");
		if (!f.exists())
			return null;

		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
			Table t = (Table) ois.readObject();
			ois.close();
			return t;
		} catch (IOException e) {
			throw new DBAppException(e.getMessage());
		} catch (ClassNotFoundException e) {
			throw new DBAppException(e.getMessage());
		}

	}
}


