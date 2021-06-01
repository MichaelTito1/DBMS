import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class testing {

	public static void main(String[] args) throws Exception{
//		DBApp session = new DBApp();
//		session.init();
//
//		String name = "Student";
		Hashtable<String, String> colnametype = new Hashtable<String, String>();
//		Hashtable<String, String> colnamemin = new Hashtable<String, String>();
//		Hashtable<String, String> colnamemax = new Hashtable<String, String>();
//
		colnametype.put("ID", "Integer");
		colnametype.put("Name", "String");
		Set<String> st = colnametype.keySet();
		System.out.println(st.toString());
//
//		colnamemin.put("ID", "1");
//		colnamemin.put("Name", "A");
//
//
//		colnamemax.put("ID", "50");
//		colnamemax.put("Name", "ZZZZZZZZZZZZ");
//
//		String key = "ID";
//
//		session.createTable(name, key, colnametype, colnamemin, colnamemax);
	}
}
