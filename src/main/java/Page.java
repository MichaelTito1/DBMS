import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable{
	private static final long serialVersionUID = 1L;
	private int maxSize, numElements;
	private String path;
	// VECTOR OF TUPLES
	private Vector<Row> row;
	
	public Page(int maxSize, String path) throws DBAppException {
		this.path = path;
		this.maxSize = maxSize;
		row = new Vector<Row>();
		save();
	}
	
	public boolean isFull(){
		if(row.size() < maxSize)
			return false;
		else
			return true;
	}
	
	public int size(){
		return row.size();
	}
	
	public Row getRow(int idx){
		return row.get(idx);
	}
	
	public Vector<Row> getRow() {
		return row;
	}
	
	private void save() throws DBAppException {
		// TODO Auto-generated method stub
		File f = new File(path);
		
		if(f.exists())
			f.delete();
		
		// Serializing the table
		try{
		ObjectOutputStream outStr = new ObjectOutputStream(new FileOutputStream(f));
		outStr.writeObject(this);
		outStr.close();
		}catch(Exception e){
			throw new DBAppException(e.getMessage());
		}
	}
	
	public void addRow(Row input) throws DBAppException {
		if(isFull())
			throw new DBAppException("the memory is full free some space first");
		
//		row[numElements] = input;
		row.add(input);
		numElements++;
		save();
	}

	public void addRow(Row input, int idx) throws DBAppException {
		row.insertElementAt(input, idx);
		numElements++;
		save();
	}
	
//	public void addRow(Row input, int idx) throws FileNotFoundException, IOException{
//		row.set(idx, input);
//		numElements++;
//		save();
//	}
	
	// Replaces a row in a given index
//	public void insertRow(Row r, int idx) throws DBAppException {
//		row.set(idx, r);
//		save();
//	}

	public void deleteRow(int j) throws DBAppException {
		row.remove(j);
		numElements--;
		save();
	}

	public String getPath() {
		return path;
	}
}
