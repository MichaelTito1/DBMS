import java.io.Serializable;
import java.util.Vector;

public class Row implements Serializable{
	private static final long serialVersionUID = 1L;
	private Object [] val;
	private Vector<String> colNames;

	public Row(int size, Vector<String> colNames){
		val = new Object [size];
		this.colNames = colNames;
	}
	
	public void addValue(int index,Object value){
		val[index]=value;
	}
	
	public Object getValue(int idx){
		return val[idx];
	}

	public Vector<String> getColNames() {
		return colNames;
	}

}