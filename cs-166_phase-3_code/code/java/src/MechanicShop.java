/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "#BlackpinkLisa1");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddCustomer(MechanicShop esql){//1 
		int id; 
		String fname;
		String lname;
		String phone;
		String address; 
		while (true) {
			System.out.println("Enter customer id ");

			try {
				id = Integer.parseInt(in.readLine());
				break;
				
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				continue; 
			}
		}
		while (true) {
			System.out.println("Enter customer's first name");
			try {
				fname = in.readLine(); 
				if (fname.length() <= 0 || fname.length() > 32) {
					throw new RuntimeException("First name has to be between 1-32 characters long");
				}
				break;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				continue; 
			}
		}
		while (true) {
			System.out.println("Enter customer's last name ");
			try {
				lname = in.readLine(); 
				if (lname.length() <= 0 || lname.length() > 32) {
					throw new RuntimeException("Last name has to be between 1-32 characters long");
				}	
				break; 
			}
			catch (Exception e) {
				System.err.println(e.getMessage()); 
				continue; 
			}
		}
		while (true) {
			System.out.println("Enter customer's phone number"); 
			try {
				phone = in.readLine();
				if (phone.length() <= 0 || phone.length() > 13) {
					throw new RuntimeException("Phone has to be between 1-13 characters long");
				}
				break;
			}
			catch (Exception e){
				System.err.println(e.getMessage()); 
				continue; 
			}
		}
		while (true) {
			System.out.println("Enter customer's address");
			try {
				address = in.readLine(); 
				if (address.length() <= 0 || address.length() > 256) {
					throw new RuntimeException("Address has to be between 1-256 characters long");
				}
				break; 
			}
			catch (Exception e) {
				System.err.println(e.getMessage()); 
				continue; 
			}
		}
		String query; 
		query = "INSERT INTO Customer (id, fname, lname, phone, address) VALUES (" + id + ", \' " + fname + " \', \' " + lname + "\', \'" + phone + "\', \'" + address + "\');" ;
		//System.out.println(query);
		while (true) {
			try {
				esql.executeUpdate(query);
				break;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}

	}
	
	
	public static void AddMechanic(MechanicShop esql){//2
		int id;
		String fname;
		String lname;
		int experience;
		
		do {
			System.out.print("Enter Mechanic ID Number: ");
			try {
				id = Integer.parseInt(in.readLine());
				break;
			}
			catch (Exception e) {
				System.out.println("Your input is Invalid!");
				continue;
			}
		}while(true);
	
		do {
			System.out.print("Enter Mechanic First Name: ");
			try {
				fname = in.readLine();
				if(fname.length() <= 0 || fname.length() > 32) {
				throw new RuntimeException("First Name cannot be NULL or over 32 characters");	
				}
	
				break;
			}
			catch (Exception e) {
				System.out.println("Your input is Invalid!");
				continue;
			}
		}while(true);	
	
		do {
			System.out.print("Enter Mechanic Last Name: ");
			try {
				lname = in.readLine();
				if(lname.length() <= 0 || lname.length() > 32) {
				throw new RuntimeException("Last Name cannot be NULL or over 32 characters");	
				}

				break;
			}
			catch (Exception e) {
				System.out.println("Your input is Invalid!");
				continue;
			}
		}while(true);	

		do {
			System.out.print("Enter Mechanic Experience: ");
			try {
				experience = Integer.parseInt(in.readLine());
				break;
			}
			catch (Exception e) {
				System.out.println("Your input is Invalid!");
				continue;
			}
		}while(true);

		try {	
			esql.executeUpdate("INSERT INTO Mechanic (id, fname, lname, experience) VALUES (" + id + ", \'" + fname + "\', \'" + lname + "\', \'" + experience + "\');" );
		}catch (Exception e) {
			System.err.println (e.getMessage());		
		}
	}
	
	public static void AddCar(MechanicShop esql){//3 
		String vin;
		String make; 
		String model; 
		int year; 

		while (true ) {
			System.out.println("Enter car's vin ");
			try {
				vin = in.readLine();
				if (vin.length() <= 0 || vin.length() > 16 ) {
					throw new RuntimeException("vin must contain 1-16 characters"); 
				}
				break;
			}
			catch(Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}
		while (true) {
			System.out.println("Enter Car's make ");
			try {
				make = in.readLine(); 
				if (make.length() <= 0 || make.length() > 32) {
					throw new RuntimeException("make must contain 1-32 characters");

				}
				break;
			}
			catch(Exception e) {
				System.err.println(e.getMessage()); 
				continue;
			}
		}
		while(true) {
			System.out.println("Enter car's model"); 
			try {
				model = in.readLine(); 
				if (model.length() <= 0 || model.length() > 32) {
					throw new RuntimeException("model must contain 1 - 32 characters"); 

				}
				break;
			}
			catch(Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}
		while(true) {
			System.out.println("Enter year of the car ");
			try {
				year = Integer.parseInt(in.readLine());
				if (year < 1970) {
					throw new RuntimeException("year must be greater than or equal to 1970");
				}
				break;
			}
			catch(Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}

		String query = "INSERT INTO Car(vin, make, model, year ) VALUES (\'" + vin + "\', \'" + make + "\',\' " + model + "\', " + year + ");";
		//System.out.println(query);
		try {
			esql.executeUpdate(query); 
		}
		catch(Exception e ) {
			System.err.println(e.getMessage());
		}
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4 
		int rid; 
		int customer_id;
		String car_vin; 
		String date; 
		int odometer; 
		String complain;

		while(true) {
			System.out.println("Enter rid for service request");
			try {
				rid = Integer.parseInt(in.readLine()); 
				break;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}
		while(true) {
			System.out.println("Enter customer id for service request"); 
			try {
				customer_id = Integer.parseInt(in.readLine()); 
				break;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}
		while(true) {
			System.out.println("Enter car vin for service request");
			try {
				car_vin = in.readLine(); 
				if (car_vin.length() <= 0 || car_vin.length() > 16) {
					throw new RuntimeException("VIN must have 1-16 characters"); 
				}
				break;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}
		while(true) {
			System.out.println("Enter date of service request"); 
			try {
				date = in.readLine();
				if (date.length() != 10 && date.charAt(4) != '-' && date.charAt(7) != '-'  ) {
					throw new RuntimeException("date must be in the format year-month-day");
				}
				break;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}
		while(true) {
			System.out.println("Enter odometer reading");
			try {
				odometer = Integer.parseInt(in.readLine());
				if(odometer <= 0 ) {
					throw new RuntimeException("Odometer reading must be a postive number"); 
				}
				break;
			}
			catch (Exception e) {
				System.err.println(e.getMessage());
				continue;
			}
		}
		while(true) {
			System.out.println("Enter complaint"); 
			try {
				complain = in.readLine();
				break;
			}
			catch (Exception e) {
				System.err.println(e.getMessage()); 
				continue;
			}
		}
		String query = "INSERT INTO Service_Request(rid, customer_id, car_vin, date, odometer, complain ) VALUES(" + rid + ", " + customer_id + ", \'" + car_vin + "\', \'" + date + "\', " + odometer + ", \'" + complain + "\');";
		//System.out.println(query);
		try {
			esql.executeUpdate(query);
		}
		catch(Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
		int wid;
		int rid;
		int mid;
		int date;
		String comment;
		int bill;
		int temp;
		int newWID;

		do {
			System.out.print("Enter a service Request Number: ");
			try {
				rid = Integer.parseInt(in.readLine());	
				temp = esql.executeQuery("SELECT * FROM Service_Request WHERE Service_Request.rid = " + rid + ";");
				if (temp == 0) {
					System.out.println("Service request number does not exist");
					continue;
				}		
				break;
			}catch (Exception e) {
				System.out.println("Your input is Invalid!");
				continue;
			}
			
		
		}while(true);

		do {
			System.out.print("Enter Mechanic ID: ");
			try{
				mid = Integer.parseInt(in.readLine());
				temp = esql.executeQuery("SELECT * FROM Mechanic WHERE Mechanic.id = " + mid + ";");
				if (temp == 0) {
					System.out.println("Mechanic ID does not exist.");
					continue;
				}
				break;
			}catch (Exception e) {
				System.out.println("Your input is Invalid!");
				continue;
			}
	
		}while(true);	
		
		do {
			System.out.print("Enter comments about repair: ");
			try {
				comment = in.readLine();
				break;
			}catch (Exception e) {
				System.out.println("Your input is Invalid!");
				continue;
			}			
		}while(true);

		do {
			System.out.print("Enter bill amount to the customer: ");
			try {
				bill = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is Invalid!");
				continue;
			}
		}while(true);
			
		newWID = esql.executeQueryAndReturnResult("SELECT wid FROM Closed_Request").size() + 1;
		
		esql.executeUpdate("INSERT INTO Closed_Request(wid, rid, mid, date, comment, bill) VALUES(" + newWID + ", " + rid + ", " + mid + ", CURRENT_DATE, \'" + comment + "\', " + bill +  ");"); 
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		try {
			esql.executeQueryAndPrintResult("SELECT Customer.fname, Customer.lname, Closed_Request.bill FROM Customer, Closed_Request, Service_Request WHERE Closed_Request.bill < 100 AND Closed_Request.rid = Service_Request.rid AND Service_Request.customer_id = Customer.id;" );
		}catch (Exception e) {
			System.err.println(e.getMessage());
		}		
	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		try {
			esql.executeQueryAndPrintResult("SELECT total.fname, total.lname, total.numCars FROM (SELECT Owns.customer_id, Customer.fname, Customer.lname, COUNT(*) numCars FROM Owns, Customer WHERE Customer.id = Owns.customer_id GROUP BY Owns.customer_id,Customer.fname,Customer.lname) AS total WHERE numCars > 20;");
		}catch(Exception e) {
			System.err.println(e.getMessage());
		}	
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		try {
			esql.executeQueryAndPrintResult("SELECT Car.vin, Car.make, Car.model, Car.year, Service_Request.odometer FROM Car, Service_Request WHERE Car.vin = Service_Request.car_vin AND Car.year < 1995 AND Service_Request.odometer < 50000;");
		}catch(Exception e) {
			System.err.println(e.getMessage());
		}	
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		//
		String query = "SELECT * FROM Car c,(SELECT s.car_vin, MAX(count.scount) FROM Service_Request s, (SELECT car_vin, COUNT(rid) AS scount FROM Service_Request GROUP BY car_vin) AS count GROUP BY s.car_vin ) AS s2 WHERE c.vin = s2.car_vin;"; 
		System.out.println("Listing Cars with the most services");
		try{
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s):" + rowCount);
		}
		catch (Exception e){
			System.err.println(e.getMessage()); 
		}
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//10
		//
		try {
			String query = "SELECT * FROM customer c, (SELECT sr.customer_id, SUM(cr.bill) AS totalBill FROM service_request sr, closed_request cr WHERE sr.rid = cr.rid GROUP BY sr.customer_id) AS c2 WHERE c.id = c2.customer_id ORDER BY c2.totalBill DESC; " ;
			int rowCount = esql.executeQueryAndPrintResult(query);
			System.out.println("total row(s): " + rowCount);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
	
}