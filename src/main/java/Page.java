package main.java;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Page implements Serializable{
	//private static final long serialVersionUID = 1L;
	private int maxSize, numElements;
	private String path;
	// VECTOR OF TUPLES
	
	public Page(int maxSize, String path) throws FileNotFoundException, IOException {
		this.path = path;
		this.maxSize = maxSize;
		// Create array[MaxSize] of tuples;
		save();
	}
	
	
	private void save() throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		File f = new File(path);
		
		if(f.exists())
			f.delete();
		
		// Serializing the table
		ObjectOutputStream outStr = new ObjectOutputStream(new FileOutputStream(f));
		outStr.writeObject(this);
		outStr.close();
	}
}
