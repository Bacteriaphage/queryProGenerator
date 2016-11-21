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
import java.io.FileWriter;
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
		//Iterator it = MFstructure.entrySet().iterator();
		/*while(it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			System.out.println(entry.getValue() + " " + entry.getKey());
		}*/
		File file = new File("sdap.pgc");
		if(file.exists()){
			System.out.println("file already exists!");
		}
		else{
			try{
				file.createNewFile();
				FileWriter fileWriter = new FileWriter(file);
				outputFrame(fileWriter);
				outputMFstruct(MFstructure, fileWriter);
				fileWriter.close();
			}catch(IOException e){
				e.printStackTrace();
			}
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
	static void outputFrame(FileWriter fileWriter) throws IOException{
		String output = new String("#include<stdio.h>\n#include<string.h>\n\n");
		output += "EXEC SQL BEGIN DECLARE SECTION;\n";
		output += "struct{\nchar\t*cust;\nchar\t*prod;\nshort\tdd;\nshort\tmm;\nshort\tyy;\nchar\t*state;\nlong\tquant;\n} sale_rec;\n";
		output += "EXEC SQL END DECLARE SECTION;\n";
		output += "EXEC SQL INCLUDE sqlca;\n\n";
		try{
			fileWriter.write(output);
			System.out.println("finish to build basic framework!");
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	static void outputMFstruct(HashMap<String, String> MFstructure, FileWriter fileWriter) throws IOException{
		String output = new String("//MFStructure\n");
		output += "struct Data{\n";
		Iterator it = MFstructure.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry entry = (Map.Entry) it.next();
			if(entry.getValue().equals("character varying")){
				output += "\t"+"char"+"\t"+entry.getKey()+"[20];\n";
			}
			else if(entry.getValue().equals("integer")){
				output += "\t"+"int"+"\t"+entry.getKey()+";\n";
			}
			else if(entry.getValue().equals("character")){
				output += "\t"+"char"+"\t"+entry.getKey()+"[3];\n";
			}
			else{
				System.out.println("some types unknown");
			}
		}
		output += "};\n";
		try{
			fileWriter.write(output);
			System.out.println("finish to build MFstructure!");
		}catch(IOException e){
			e.printStackTrace();
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
			for(String obj:myQuery.groupingAttri){
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
			for(String obj:myQuery.aggreFunc){
				ResultSet rs;
				Statement st = con.createStatement();
				String ret = "select * from information_schema.columns where table_name = 'sales' and column_name = '";
				//ret.concat("where table_name = 'sales' and column_name = '");
				String[] tmp = obj.split("_");
				ret += tmp[tmp.length-1];
				ret += "'";
				rs = st.executeQuery(ret);
				rs.next();
				if(tmp[0].equals("avg")){
					String sum = new String("sum");
					String count = new String("count");
					if(tmp.length > 1){
						sum += "_"+tmp[1]+"_"+tmp[2];
						count += "_"+tmp[1]+"_"+tmp[2];
					}
					MFstructure.put(sum, rs.getString(8));
					MFstructure.put(count, rs.getString(8));
				}
				else{
					MFstructure.put(obj, rs.getString(8));
				}
			}
		}catch(Exception exception){
			System.out.println("Fail to build MFstructure");
			exception.printStackTrace();
		}
	}
}
/*
 while loop
while(1)
	 {
	 if(EOT) break;
	 if(condition satisfied){
	 	if(grouping attribute in MF-Struct){
	 	update the corresponding tuple;
	 	}
	 	else{
	 	add a new tuple to MF-Structure;
	 	initial new tuple;
	 	}
	 } 
 }
 */
/*
 output
 from MF_Structure
 based on 
 Selected Attribute1 and having clause 6
 */


