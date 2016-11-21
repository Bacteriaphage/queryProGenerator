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
				outputMainFunc(fileWriter);
				outputProcessFunc(fileWriter, myQuery);
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
	static void outputProcessFunc(FileWriter fileWriter, Query myQuery){
		String output = new String("void process(struct Data *data, int *i){\n");
		int aggreFuncIndex = 0;
		int suchThatIndex = 0;
		int havingIndex = 0;
		for(int NGV = 0; NGV <= myQuery.numOfGV; NGV++){
			if(NGV == 0){
				String[] temp = myQuery.aggreFunc.get(0).split("_");
				if(temp[1] != "0") continue; 
			}
			output += "\tEXEC SQL DECLARE mycursor CURSOR FOR SELECT * FROM sales;\n";
			output += "\tEXEC SQL SET TRANSACTION read only;\n";
			output += "\tEXEC SQL OPEN mycursor;\n";
			output += "\tEXEC SQL FETCH FROM mycursor INTO :sale_rec;\n";
			output += "\twhile(sqlca.sqlcode == 0){\n";
			boolean condi = true;
			if(NGV == 0){
				String[] temp = myQuery.aggreFunc.get(aggreFuncIndex).split("_");
				if(temp[1].equals("0")){
					condi = false;
				}
			}
			output += "\t\t"+"int j;\n";
			output += "\t\t"+"for(j = 0; j < *i; j++)\n";
			output += "\t\t\t"+"if(";
			for(String obj : myQuery.groupingAttri){
				output += "strcmp(data[j]."+obj+", sale_rec."+obj+")==0&&";
			}
			output = output.substring(0, output.length()- 2);
			output += ") break;\n";
			output += "\t\t\t" + "if(j == *i){\n";
			if(condi){
				output += "\t\t\t\tif(";
				while(true){
					String[] temp = myQuery.suchThat.get(suchThatIndex).split("_");
					System.out.println(temp[0]);
					if(!(temp[0].equals(Integer.toString(NGV)))) break;
					String GVattri = new String();
					String OBJattri = new String();
					String operator = new String();
					int sign = 0;
					for(;temp[1].charAt(sign)>='a' && temp[1].charAt(sign)<='z' || 
							temp[1].charAt(sign)>='A' && temp[1].charAt(sign)<='Z' || 
							temp[1].charAt(sign)>='0' && temp[1].charAt(sign)<='9'; 
							sign++){
						GVattri += temp[1].charAt(sign);
					}
					while(!(temp[1].charAt(sign)>='a' && temp[1].charAt(sign)<='z' || 
							temp[1].charAt(sign)>='A' && temp[1].charAt(sign)<='Z' ||
							temp[1].charAt(sign)>='0' && temp[1].charAt(sign)<='9')){
						operator += temp[1].charAt(sign);
						sign++;
					}
					for(;sign < temp[1].length(); sign++){
						OBJattri += temp[1].charAt(sign);
					}
					if(temp.length == 4){
						OBJattri += "_" + temp[2] + "_" + temp[3];
						output += "sale_rec." + GVattri + operator + "data[j]."+OBJattri;
					}
					else{
						output += "sale_rec." + GVattri + operator + OBJattri;
					}
					String next = myQuery.suchThat.get(suchThatIndex + 1);
					
					if(next.equals("and") || next.equals("or")){
						if(next.equals("and")){
							output += " && ";
						}
						else{
							output += " || ";
						}
						suchThatIndex += 2;   //get next condition;
					}
					else{
						break;
					}
				}
				output += "){\n";
			}
			for(String obj : myQuery.groupingAttri){
				output += "\t\t\t\tstrcpy(data[*i]." + obj + ", "+ "sale_rec." + obj +");\n";
			}
			if(condi)
				output += "\t\t\t\t}\n";
			
		}
		try{
			fileWriter.write(output);
			System.out.println("finish to parse SQL!");
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	static void outputMainFunc(FileWriter fileWriter) throws IOException{
		String output = new String("int main(int argc, char* argv[])\n");
		output += "{\n\tstruct Data data[500];\n\tint counter = 0;\n";
		output += "\tEXEC SQL CONNECT TO postgres@localhost:5432 USER postgres IDENTIFIED BY zhy199208;\n";
		output += "\tif(sqlca.sqlcode != 0){\n\t\tprintf(\"Login error!!!\\n\");\n";
		output += "\t\treturn -1\n";
		output += "\t}\n";
		output += "\tEXEC SQL WHENEVER sqlerror sqlprint;\n";
		output += "\tprocess(data, &counter);\n";
		output += "\toutput(data, counter);\n";
		output += "\treturn 0;\n";
		output += "}\n";
		try{
			fileWriter.write(output);
			System.out.println("finish to build main function!");
		}catch(IOException e){
			e.printStackTrace();
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
			String line5 = buf.readLine();                             //condition for different GV use space to separate.
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
					MFstructure.put(obj, rs.getString(8));
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


