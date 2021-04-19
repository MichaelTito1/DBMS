package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

public class DBApp implements DBAppInterface{

	private File metadata;
	private Properties prop;
	//private String mainDir = "DB_app/";  //WILL THE SPACE CAUSE ERRORS?!!!!!
	private String mainDir = "tables\\";
	private Vector<String> datatypes = new Vector<String>();
	@Override
	public void init() throws FileNotFoundException, IOException {
		
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
		fetchConfigFile();
		
		// Getting the metadata file
		this.metadata = new File("src//main//resources//metadata.csv");

		// make a new directory for the tables
		File f = new File(mainDir);
		f.mkdirs();
		
		// add supported column datatypes
		datatypes.add("Integer");
		datatypes.add("String");
		datatypes.add("Double");
		datatypes.add("Date");
		
	}

	@Override
	public void createTable(String tableName, String clusteringKey,
			Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin,
			Hashtable<String, String> colNameMax) throws DBAppException, IOException {
		
		// Check if this table name already exists
		File f = new File(mainDir+tableName+".class");
		if(f.exists())
			throw new DBAppException("Table " + tableName + " already exists. Please choose a different name.");
		
		// check if all column types are supported
		checkColTypes(colNameType);
		
		// add table info to the metadata.csv file
		addTableToMeta(tableName, colNameType, colNameMin, colNameMax, clusteringKey);
		
		// Create the new table 
		int maxPageSize = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		System.out.println(maxPageSize);
		new Table(mainDir, tableName, clusteringKey, maxPageSize, colNameType, colNameMin, colNameMax);
		System.out.println("Table " + tableName + " is created successfully");
	}

	@Override
	public void createIndex(String tableName, String[] columnNames)
			throws DBAppException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insertIntoTable(String tableName,
			Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue,
			Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteFromTable(String tableName,
			Hashtable<String, Object> columnNameValue) throws DBAppException {
		// TODO Auto-generated method stub
		
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
			String clusteringKey) throws IOException
	{
		PrintWriter pr = new PrintWriter(new FileWriter(metadata, true));
		
		for(Entry<String, String> entry : colNameType.entrySet()){
			String colName = entry.getKey();
			String colType = entry.getValue();
			boolean isPrimary = colName.equals(clusteringKey);
			pr.append(tableName+","+colName+","+colType+","+isPrimary+","+"false"+","+colNameMin.get(colName)+","+colNameMax.get(colName)+","+"\n");
		}
		pr.flush();
		pr.close();
	}
	
	private void fetchConfigFile() throws IOException, FileNotFoundException{
		
		this.prop = new Properties();
	    String fileName = "DBApp.config";
	    InputStream is = new FileInputStream(fileName);

	    this.prop.load(is);
	    
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
	
	private void checkColTypes(Hashtable<String , String> htblColNameType) throws DBAppException{
		for(Entry<String, String> e : htblColNameType.entrySet()){
			if(!datatypes.contains(e.getValue()))
				throw new DBAppException("Invalid Column: " + e.getKey() + " Datatype : " + e.getValue());
		}
	}

}


