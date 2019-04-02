import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Map;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*; 

import org.apache.hadoop.conf.Configuration; 
/**
 *complie HDFSTest.java
 *
 * javac HDFSTest.java 
 *
 *execute HDFSTest.java
 *
 * java HDFSTest  
 * 
 */

public class Hw1Grp4 {
public static void HBaseTest(HashMap<String,String> map,String[] colName,String[] colNum) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

    Logger.getRootLogger().setLevel(Level.WARN);

    // create table descriptor
    String tableName= "Result";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

    // create column descriptor
    HColumnDescriptor cf = new HColumnDescriptor("res");
    htd.addFamily(cf);

    // configure HBase
    Configuration configuration = HBaseConfiguration.create();
    HBaseAdmin hAdmin = new HBaseAdmin(configuration);


    //delete the table existed 
	if (hAdmin.tableExists(tableName)) {
	        //System.out.println("Table already exists");
			DeleteTable(tableName);
			hAdmin.createTable(htd);
		    System.out.println("table "+tableName+ " created successfully");
    }
	else {
	        hAdmin.createTable(htd);
	        System.out.println("table "+tableName+ " created successfully");
	}
	    hAdmin.close();


    //put the data from hashMap into HBase database
	//because of the variable of input parameters, we should make a for cycle 
	Set<String> set=map.keySet();
	Iterator it=set.iterator();
	int i=0;
	HTable table = new HTable(configuration,tableName);
	while(it.hasNext()){
		String key=(String) it.next();
		String[] col=key.split("\\|");
		String j=String.valueOf(i);
      	Put put = new Put(j.getBytes());		
/*		put.add("res".getBytes(),colName[1].getBytes(),col[0].getBytes());
   	    table.put(put);
		put.add("res".getBytes(),colName[2].getBytes(),col[1].getBytes());
   	    table.put(put);
		put.add("res".getBytes(),colName[3].getBytes(),col[2].getBytes());	
   	    table.put(put);
*/
		for(int x=1;x<colName.length;x++){
			put.add("res".getBytes(),colName[x].getBytes(),colNum[x-1].getBytes());
			table.put(put);
		}
		i++;		
	}
    table.close();
    System.out.println("put successfully");
  }



	public static void DeleteTable(String tablename) throws IOException{
		
		//Instantiating configuration class
		Configuration conf=HBaseConfiguration.create();

		//Instantiating HBaseAdmin class
		HBaseAdmin admin=new HBaseAdmin(conf);

		//disabling table named emp
		admin.disableTable(tablename);

		//Deleting emp
		admin.deleteTable(tablename);
		System.out.println("Table deleted");
	}




    public static void main(String[] args) throws IOException, URISyntaxException{
		//processing the argument lists from String[] args
		//and making it more general
		//args[0]
		String[] fileName=args[0].split("=");
		//args[1]
		String[] req=args[1].split("\\:|\\,");
		String[] bendan=req[1].split("R");
		Integer num=Integer.valueOf(bendan[1]);
		//args[2]
/*
		String[] colName=args[2].split("\\:|\\,");
		String[] name1=(colName[1].split("R"));
		String[] name2=(colName[2].split("R"));
		String[] name3=(colName[3].split("R"));
		Integer col1=Integer.valueOf(name1[1]);
		Integer col2=Integer.valueOf(name2[1]);
		Integer col3=Integer.valueOf(name3[1]);
*/


		String[] colName=args[2].split(":|,");
		String[] colNum=new String[colName.length-1];//there is colName.length-1 not colName.length-2 yuejie yichu
		for(int k=1;k<colName.length;k++){
			colNum[k-1]=(colName[k].split("R"))[1];
		}






        String file= "hdfs://localhost:9000"+fileName[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));


		//analyze the input parameter, then transfer into the proper action.
        String s;
		HashMap<String,String> map=new HashMap<String,String>();
        while ((s=in.readLine())!=null) {
		    String[] strArray = s.split("\\|");
		    String strAppend=null;

			boolean p=false;
			//String symbol="eq";//shi bu shi fang dao wai mian geng hao???
			if(req[2].compareTo("gt")==0){
				if(Float.valueOf(strArray[num])>Float.valueOf(req[3])){
					p=true;
				}
			}
			else if(req[2].compareTo("ge")==0){
				if((Float.valueOf(strArray[num])>Float.valueOf(req[3]))||Math.abs(Float.valueOf(strArray[num])-Float.valueOf(req[3]))<1e-6){
					p=true;
				}
			}
			else if(req[2].compareTo("eq")==0){
				if(Math.abs(Float.valueOf(strArray[num])-Float.valueOf(req[3]))<1e-6){
					p=true;
				}
			}
			else if(req[2].compareTo("ne")==0){

				if(!(Math.abs(Float.valueOf(strArray[num])-Float.valueOf(req[3]))<1e-6)){
				//if(Float.valueOf(strArray[num])!=Float.valueOf(req[3])){
					p=true;
				}
			}
			else if(req[2].compareTo("le")==0){
				if((Float.valueOf(strArray[num])<Float.valueOf(req[3]))||Math.abs(Float.valueOf(strArray[num])-Float.valueOf(req[3]))<1e-6){
					p=true;
				}
			}
			else if(req[2].compareTo("lt")==0){
				if(Float.valueOf(strArray[num])<Float.valueOf(req[3])){
					p=true;
				}
			}


			//make the required data into the hashMap/hashTable, which will nake the data distinct. 
		    if(p) {	
				for(int f=0;f<colNum.length;f++){
					strAppend=strAppend+strArray[Integer.valueOf(colNum[f])]+"|";
				}
				map.put(strAppend,null);
		        //map.put(strArray[col1]+"|"+strArray[col2]+"|"+strArray[col3]+"|",null);		        
		    }
        }
		HBaseTest(map,colName,colNum);



        in.close();

        fs.close();
    }
}
