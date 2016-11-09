package queryProGenerator;
import java.util.*;
import java.sql.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
public class queryProGenerator {
	String usr = "postgres";
	String pwd = "zhy199208";
	String url = "jdbc:postgresql://localhost:5432/sales";
	
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
		HashMap<String, String> MFstructure = new HashMap<String,String>();
		task.inputQuery(myQuery);
		task.buildStruct(myQuery,MFstructure);
		Iterator it = MFstructure.entrySet().iterator();
/*		while(it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			System.out.println(entry.getValue() + " " + entry.getKey());
		}*/
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
	void buildStruct(Query myQuery, HashMap<String, String> MFstructure){
		try{
			Connection con = DriverManager.getConnection(url, usr, pwd);
			System.out.println("Success connecting server!");
			for(String obj:myQuery.select){
				ResultSet rs;
				Statement st = con.createStatement();
				String ret = "select * from information_schema.columns where table_name = 'sales' and column_name = '";
				//ret.concat("where table_name = 'sales' and column_name = '");
				String[] tmp = obj.split("_");
				ret += tmp[tmp.length-1];
				ret += "'";
				rs = st.executeQuery(ret);
				rs.next();
				MFstructure.put(obj, rs.getString(8));
			}
		}catch(Exception exception){
			System.out.println("Fail to build MFstructure");
			exception.printStackTrace();
		}
	}
		
}
