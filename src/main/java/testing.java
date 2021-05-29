import java.util.Hashtable;

public class testing {

	public static void main(String[] args) throws Exception{
		DBApp session = new DBApp();
		session.init();

		String name = "Student";
		Hashtable<String, String> colnametype = new Hashtable<String, String>();
		Hashtable<String, String> colnamemin = new Hashtable<String, String>();
		Hashtable<String, String> colnamemax = new Hashtable<String, String>();

		colnametype.put("ID", "Integer");
		colnametype.put("Name", "String");

		colnamemin.put("ID", "1");
		colnamemin.put("Name", "A");


		colnamemax.put("ID", "50");
		colnamemax.put("Name", "ZZZZZZZZZZZZ");

		String key = "ID";

		session.createTable(name, key, colnametype, colnamemin, colnamemax);
	}
}
