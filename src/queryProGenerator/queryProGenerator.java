package queryProGenerator;
import java.util.*;
import java.sql.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class queryProGenerator {
	String usr = "postgres";
	String pwd = "zhy199208";
	String url = "jdbc:postgresql://localhost:5432/sales";
	
	static public class MFstructure{
		String typeName;
		String attriName;
		MFstructure(String type, String attri){
			typeName = type;
			attriName = attri;
		}
		MFstructure(){}
	}
	static public class Query{
		ArrayList<String> select;
		int numOfGV;
		ArrayList<String> groupingAttri;
		ArrayList<String> aggreFunc;
		ArrayList<String> suchThat;
		ArrayList<String> havingCondi;
		Query(){
			select = new ArrayList<String>();
			groupingAttri = new ArrayList<String>();
			aggreFunc = new ArrayList<String>();
			suchThat = new ArrayList<String>();
			havingCondi = new ArrayList<String>();
		}
	}
	
	public static void main(String[] args) throws IOException {
		queryProGenerator task = new queryProGenerator();
		task.connect();
		Query myQuery = new Query();
		ArrayList<MFstructure> myStructure;
		task.inputQuery(myQuery);
		for(Object obj:myQuery.select){
			System.out.print(obj);
		}
		System.out.print("\n");
		System.out.println(myQuery.numOfGV);
		for(Object obj:myQuery.groupingAttri){
			System.out.print(obj);
		}
		System.out.print("\n");
		for(Object obj:myQuery.aggreFunc){
			System.out.print(obj);
		}
		System.out.print("\n");
		for(Object obj:myQuery.suchThat){
			System.out.print(obj);
		}
		System.out.print("\n");
		for(Object obj:myQuery.havingCondi){
			System.out.print(obj);
		}
		//dbmsass1.retrieve();
	}
	//Function to connect to the database
	void connect(){
		try{
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!");
		}catch(Exception exception){
			System.out.println("Fail loading Driver!");
			exception.printStackTrace();
		}
	}
	void inputQuery(Query myQuery) throws IOException{
		InputStream in = null;
		try{
			in = new FileInputStream("query.txt");
			BufferedReader buf = new BufferedReader(new InputStreamReader(in));
			
			String line1 = buf.readLine();
			
			String[] tmp1 = line1.split(" ");
			for(int i = 0; i < tmp1.length; i++){
				myQuery.select.add(tmp1[i]);
			}
			String line2 = buf.readLine();
			if(line2 != null)
				myQuery.numOfGV = Integer.parseInt(line2);
			
			String line3 = buf.readLine();
			if(line3 != null){
				String[] tmp3 = line3.split(" ");
				for(int i = 0; i < tmp3.length; i++){
					myQuery.groupingAttri.add(tmp3[i]);
				}
			}
			String line4 = buf.readLine();
			if(line4 != null){
				String[] tmp4 = line4.split(" ");
				for(int i = 0; i < tmp4.length; i++){
					myQuery.aggreFunc.add(tmp4[i]);
				}
			}
			String line5 = buf.readLine();
			if(line5 != null){
				String[] tmp5 = line5.split(" ");
				for(int i = 0; i < tmp5.length; i++){
					myQuery.suchThat.add(tmp5[i]);
				}
			}
			String line6 = buf.readLine();
			if(line6 != null){
				String[] tmp6 = line6.split(" ");
				for(int i = 0; i < tmp6.length; i++){
					myQuery.havingCondi.add(tmp6[i]);
				}
			}
		}catch(Exception exception){
			System.out.println("Fail read query!");
			exception.printStackTrace();
		}finally{
			if(in != null){
				in.close();
			}
		}
	}
	void buildStruct(){
		
	}
	//customized function
	
	
	
	
/*	void retrieve(){
		try {
	        Connection con = DriverManager.getConnection(url, usr, pwd);    //connect to the database using the password and username
	        System.out.println("Success connecting server!");
	        ResultSet rs;          			 //resultset object gets the set of values retreived from the database
	        boolean more;
	        int i=1,j=0;
	        Statement st = con.createStatement();   //statement created to execute the query
	        String ret = "select * from sales";
	        rs = st.executeQuery(ret);              //executing the query 
	        more=rs.next();                         //checking if more rows available
	        System.out.printf("%-8s","Customer  ");             //left aligned
	        System.out.printf("%-7s","Product  ");              //left aligned
	        System.out.printf("%-5s","Day    " +
	        		"");                //left aligned
	        System.out.printf("%-10s","Month    ");          //left aligned
	        System.out.printf("%-5s","Year   ");                //left aligned
	        System.out.printf("%-10s","State    ");          //left aligned
	        System.out.printf("%-5s%n","Quant  ");              //left aligned
	        System.out.println("========  =======  =====  ========  =====  ========  =====");
	        while(more)
	        {
	        	System.out.printf("%-8s  ",rs.getString(1));            //left aligned
	            System.out.printf("%-7s  ",rs.getString(2));            //left aligned
	            System.out.printf("%5s  ",rs.getInt(3));             //right aligned
	            System.out.printf("%8s  ",rs.getInt(4));            //right aligned
	            System.out.printf("%5s  ",rs.getInt(5));             //right aligned
	            System.out.printf("%-8s  ",rs.getString(6));            //right aligned
	            System.out.printf("%5s%n",rs.getString(7));   //right aligned
	            
	        	more=rs.next();
	        }
	        } catch(SQLException e) {
	         System.out.println("Connection URL or username or password errors!");
	        e.printStackTrace();
	        }
		
	}
	*/
}
