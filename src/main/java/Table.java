package main.java;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Properties;

public class Table implements Serializable{
	
	// private static final long serialVersionUID = 1L;
	private String tableName, primaryKey, path;
	private Hashtable<String, String> htblColNameType, htblColNameMin, htblColNameMax;
	private int maxPageSize; 
	private int it = -1; //Iterator on pages
	// SHOULD WE KEEP AN ARRAY OF PAGES (SIMILAR TO WHAT WE DID WITH PAGES AND TUPLES) ??!!!!
	
	
	public Table(String path,
			String tableName,
			String primaryKey,
			int maxPageSize,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax) throws FileNotFoundException, IOException{
		
		// Initializing attributes
		this.path = path;
		this.tableName = tableName;
		this.primaryKey = primaryKey;
		this.maxPageSize = maxPageSize;
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		
		// Create directory of the table
		File dir = new File(path);
		dir.mkdirs();
		
		//Create Page
		createPage();
		
		// Saving the table in its directory
		save();
	}

	private Page createPage() throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		it++;
		Page p = new Page(maxPageSize, path+tableName+"_"+"page"+it+".class");
		save();
		return p;
	}

	private void save() throws IOException, FileNotFoundException {
		// TODO Auto-generated method stub
		File f = new File(path+tableName+".class");
		
		if(f.exists())
			f.delete();
		
		// Serializing the table
		ObjectOutputStream outStr = new ObjectOutputStream(new FileOutputStream(f));
		outStr.writeObject(this);
		outStr.close();
	}
	
}
