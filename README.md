# queryProGenerator
This project is a application for Multi-Feature Query. If you interest in this concept please check the following link:
http://dl.acm.org/citation.cfm?id=673628

The Java program will automatically generate a C program to process the input query;

## 1.MF-Query explanation
A example query is like this:
```
cust prod sum_1_quant count_1_quant

1

cust prod

avg_0_quant sum_1_quant count_1_quant

1_state="NY" and 1_quant>avg_0_quant
```

That means for each customer and product, output the sum and count of the trading quantity which traded in NY state and the quantity is greater than the average quantity of whole year for this combination of customer and product.  

*This MF-query do not have "having condition" so there are only 5 argument in it. *
## Project Diagram

![](https://github.com/Bacteriaphage/queryProGenerator/blob/master/diagram.JPG)

## Pre-Process Function explanation
#### inputQuery(Query myQuery)
This function get this query input then store these data into a Query object.

#### buildStruct(Query myQuery, HashMap< String , String > MFstructure)
This function get the Query object and connect to PostgreSql database. It use:
`select * from information_schema.columns where table_name = ... and column_name = ...`
to get the information about all attributes of one table attribute such like "cust", then we can easily get what the type of "cust".
After we finish to process all data in Query we will get all attribute and their type and store them into the MFstructure.

## Output Function explanation
#### outputProcessFunc(FileWriter fileWriter, Query myQuery, HashMap< String , String > MFstructure)
According to the Query information, we can get how many table scan we will do at most depending on the number of grouping variable. 
We use each loop to work out the C code of each grouping varible, any aggragate functions about one grouping variable can be done by one loop. In the generated C program every grouping varible need one table scan cursor (this is not true if we use topological sort to optimize the program), so we also generate enough cursors in outputFrame() function. For each table scan, we need implement such that condition if needed. Addtional, if we need a global aggregate function for example the sum of all quantity for a combination of cust and prod, we need a group variable 0. We must create a table scan progress for this grouping varible as well. All data should be store in the corralated tuple in MFstructure.

About optimization:
In some situation, we can complete several grouping varibles within only one table scan, we call these kind of varible or aggragate function as independent grouping varibles or functions. Before we generate the table scan code, we need to build a directive graph according to the "such that" argument, then use kahn's algorithm to do topological sort and figure out which grouping varibles can be done in one table scan. Without optimization the C program structure is like this:
```
if(j == *i){
	if((strcmp(sale_rec1.state,"NY")==0)){
		...
	}
}
else{
	if((strcmp(sale_rec1.state,"NY")==0)){
		...
	}
}
```
If there is a varible want to check another state, say, NJ and it is independent with NY grouping varible, the program structure will become:
```
if(j == *i){                                 //the combination of group attributes is not exist in MFstructure 
	if((strcmp(sale_rec1.state,"NY")==0)){
		...
	}
	else if((strcmp(sale_rec1.state,"NJ")==0)){
	...
	}
}
else{                                        //the combination has already been in MFstructure
	if((strcmp(sale_rec1.state,"NY")==0)){
		...
	}
	else if((strcmp(sale_rec1.state,"NJ")==0)){
	...
	}
}
```
#### outputOutputFunc(FileWriter fileWriter, Query myQuery, HashMap< String , String > MFstructure)
This function will grant the C program the logic to do output and organize the query output format. The basic idea is just scanning the MFstructure because it has all we need. During output each tuple, C program also need to deal with "having condition" to decide which tuple should be print out. So the structure of output logiv is somehow like the table scan process because one needs to tackle "having condition", the other needs to do with "such that".

#### outputFrame(FileWriter fileWriter, Query myQuery)
This function helps C program define its data structure of table scan cursor. According to the number of grouping varible, it also declare enough cursors.

#### outputMFstruct(HashMap< String , String > MFstructure, FileWriter fileWriter)
This function helps C program define its MFstructure.

#### outputMainFunc(FileWriter fileWriter)
Create main function in C program
