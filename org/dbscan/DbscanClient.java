package org.dbscan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.dbscan.getNeighbors;
import org.dbscan.getNeighbors.Data;
import org.dbscan.getNeighbors.clustersPair;
import org.dbscan.getNeighbors.clustersPair.Builder;
import org.dbscan.getNeighbors.getNeighborsRequest;
import org.dbscan.getNeighbors.getNeighborsResponse;
import org.dbscan.getNeighbors.mergeClustersRequest;
import org.dbscan.getNeighbors.mergeClustersResponse;
import org.ibm.developerworks.getRowCount.getRowCountRequest;
import org.ibm.developerworks.getRowCount.getRowCountResponse;
import org.ibm.developerworks.getRowCount.ibmDeveloperWorksService;

import com.google.common.primitives.Bytes;

public class DbscanClient {

	public int Minpt = 10;
	public double Eps = 5;
	private int nextClusterId = 1;
	private String tableName = "test3";
	private HTableInterface tblConn = null;  
	private HTableInterface metaTbl = null;
	private int countOfRow = 0;
	
	public DbscanClient(int Minpt, double Eps){
		this.Minpt = Minpt;
		this.Eps = Eps;
	}
	
	public boolean loadParameter(String tableName) throws IOException{
		 Configuration config = new Configuration();
	     HBaseAdmin admin = new HBaseAdmin(config);
	     HConnection connection = HConnectionManager.createConnection(config);
	     HTableInterface table = connection.getTable(tableName+"Metadata");
	     
	     if(admin.tableExists(tableName) && admin.tableExists(tableName + "Metadata")){
	    	 this.tableName = tableName;
	    	 Get get = new Get("1".getBytes());
	    	 get.addColumn("PARA".getBytes(), "EPS".getBytes());
	    	 get.addColumn("PARA".getBytes(), "MINPT".getBytes());
	    	 get.addColumn("PARA".getBytes(), "NEXTID".getBytes());
	    	 get.addColumn("PARA".getBytes(), "COUNT".getBytes());
	    	 Result result = table.get(get);
	    	 this.Eps = new Double(new String(result.getValue("PARA".getBytes(), "EPS".getBytes())));
	    	 this.Minpt = new Integer(new String(result.getValue("PARA".getBytes(), "MINPT".getBytes())));
	    	 this.nextClusterId = new Integer(new String(result.getValue("PARA".getBytes(), "NEXTID".getBytes())));
	    	 this.countOfRow = new Integer(new String(result.getValue("PARA".getBytes(), "COUNT".getBytes())));
	    	 if(this.tblConn != null) this.tblConn.close();
	    	 this.tblConn = null;
	    	 return true;
	     }
	     return false;
	}
	
	public void createTable(String tableName){
		try{
			 this.tableName = tableName;
	        Configuration config = new Configuration();
	        HBaseAdmin admin = new HBaseAdmin(config);
	        if(admin.tableExists(this.tableName) == true ) {
	        	
	        	if(admin.isTableEnabled(this.tableName))
	        		admin.disableTable(this.tableName);
	        	
	          admin.deleteTable(this.tableName);
	        } 
	        
	        if(admin.tableExists(this.tableName + "Metadata") == true ) {
	        	
	        	if(admin.isTableEnabled(this.tableName+"Metadata"))
	        		admin.disableTable(this.tableName+"Metadata");
	        	
	          admin.deleteTable(this.tableName+"Metadata");
	        } 
	        
	        HTableDescriptor tableDesc = new HTableDescriptor(this.tableName);
	        tableDesc.addFamily(new HColumnDescriptor("DATA")); //add column family
	        tableDesc.addFamily(new HColumnDescriptor("DBSCAN"));
	        //tableDesc.addCoprocessor("org.dbscan.coprocessor.RowCountObserver");
	        tableDesc.addCoprocessor("org.dbscan.coprocessor.DBSCANEndpoint");
	        admin.createTable(tableDesc);
	        HTableDescriptor td = new HTableDescriptor(this.tableName + "Metadata");
	        td.addFamily(new HColumnDescriptor("PARA"));
	        admin.createTable(td);
	     
	        //Configuration config = new Configuration();
            HConnection connection = HConnectionManager.createConnection(config);
      	    HTableInterface table = connection.getTable(this.tableName+"Metadata");
	        Put put = new Put("1".getBytes());
	        put.addColumn("PARA".getBytes(), "EPS".getBytes(), ("" + this.Eps).getBytes());
	        put.addColumn("PARA".getBytes(), "MINPT".getBytes(), ("" + this.Minpt).getBytes());
	        put.addColumn("PARA".getBytes(), "NEXTID".getBytes(), ("" + this.nextClusterId).getBytes());
	        put.addColumn("PARA".getBytes(), "COUNT".getBytes(), ("" + this.countOfRow+10000*Math.random()).getBytes());
	        table.put(put);
	        table.close();
	        if(this.tblConn != null) this.tblConn.close();
	        this.tblConn = null;
	      }
	      catch(Exception e) {e.printStackTrace();}
	}
	
	public void loadFile(String fileName)throws IOException{
		
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		String data = br.readLine();//一次读入一行，直到读入null为文件结束
		int counter = this.countOfRow;
		
	     System.out.println("Start load file");
		while( data!=null){
		      	String[] values = data.split(" ");	
				counter ++;
				List<Double> point = new ArrayList<Double>();
				try {
					for(String value: values){
						point.add(new Double(value));
					}
				}catch (Exception e){
					//System.out.println("123");
					;
				}
		       this.addPoint((counter+""), point);
		       
		       System.out.println("Adding: " + counter +" "+ point.toString());
			   data = br.readLine(); //接着读下一行
		}
		HTableInterface table = null;
        if(this.tblConn == null){
      	  Configuration config = new Configuration();
            HConnection connection = HConnectionManager.createConnection(config);
      	  this.tblConn = connection.getTable(this.tableName);
        }
        table = this.tblConn;
		System.out.println("End load file");
	}
	
	public double distanceFunc(List<Double> A, List<Double> B){
		double d = 0.0;
		for(int i = 0; i < A.size(); i ++){
			d += Math.pow(A.get(i)-B.get(i), 2.0);
		}
		return Math.sqrt(d);
	}
	
	public boolean isNeighbor(List<Double> A, List<Double> B){
		return distanceFunc(A, B) < this.Eps;
	}
	
    private List<getNeighbors.Data> getNeighbors(String rowKey,final double eps, final List<Double> point){
        final List<getNeighbors.Data> neighbors = new ArrayList<getNeighbors.Data>();
        try{
         
          HTableInterface table = null;
          if(this.tblConn == null){
        	  Configuration config = new Configuration();
              HConnection connection = HConnectionManager.createConnection(config);
        	  this.tblConn = connection.getTable(this.tableName);
          }
          table = this.tblConn;
          Batch.Call<getNeighbors.dbscanService, getNeighborsResponse> callable = 
           new Batch.Call<getNeighbors.dbscanService, getNeighborsResponse>() {
             ServerRpcController controller = new ServerRpcController();
             BlockingRpcCallback<getNeighborsResponse> rpcCallback = 
             new BlockingRpcCallback<getNeighborsResponse>();

             @Override
             public getNeighborsResponse call(getNeighbors.dbscanService instance) throws IOException {
             org.dbscan.getNeighbors.getNeighborsRequest.Builder builder = getNeighborsRequest.newBuilder();   
             builder.setEps(eps);
             builder.addAllData(point);
             instance.getNeighbours(controller, builder.build(), rpcCallback);
             return rpcCallback.get();
            }
           };
          Batch.Callback< getNeighborsResponse> callback = 
          new Batch.Callback<getNeighborsResponse>() {
          @Override
          public void update(byte[] region, byte[] row, getNeighborsResponse result) {
                  for(getNeighbors.Data data: result.getDatasList()){
                	  neighbors.add(data);
                  }
              }
          };

          table.coprocessorService(getNeighbors.dbscanService.class, null, null, callable, callback);
          //table.close();
          }
          catch(Exception e) {e.printStackTrace();}  
          catch(Throwable t) {;}
          return neighbors;
      }

	
	private void updateNumOfNeighbors(String rowKey, List<getNeighbors.Data> re){
		getNeighbors.Data current=null, nd = null;
		for(int i = 0;i < re.size(); i ++){
			current = re.get(i);
			if(current.getRowkey().equals(rowKey)){
				nd = Data.newBuilder(current).setNumOfNeighbors(re.size()).build();
			}else{
				nd = Data.newBuilder(current).setNumOfNeighbors(current.getNumOfNeighbors() + 1).build();
			}
			re.set(i, nd);
		}
	}
	
	private void mergeClusterInHbase(final TreeMap<Integer, Integer>  newClusterIdMap){
		if(newClusterIdMap.size() == 0) return;
        try{
          //HTableInterface table = connection.getTable(this.tableName);
          HTableInterface table = null;
          if(this.tblConn == null){
        	  Configuration config = new Configuration();
              HConnection connection = HConnectionManager.createConnection(config);
        	  this.tblConn= connection.getTable(this.tableName);
          }
          
          table = this.tblConn;
          
          Batch.Call<getNeighbors.dbscanService, getNeighbors.mergeClustersResponse> callable = 
           new Batch.Call<getNeighbors.dbscanService, getNeighbors.mergeClustersResponse>() {
             ServerRpcController controller = new ServerRpcController();
             BlockingRpcCallback<getNeighbors.mergeClustersResponse> rpcCallback = 
             new BlockingRpcCallback<getNeighbors.mergeClustersResponse>();

             @Override
             public getNeighbors.mergeClustersResponse call(getNeighbors.dbscanService instance) throws IOException {
             org.dbscan.getNeighbors.mergeClustersRequest.Builder builder = mergeClustersRequest.newBuilder();   
             org.dbscan.getNeighbors.clustersPair.Builder Pair = clustersPair.newBuilder();
             for(Map.Entry<Integer, Integer> entry: newClusterIdMap.entrySet()){
            	 Pair.setCluster1(entry.getKey());
            	 Pair.setCluster2(entry.getValue());
            	 builder.addClustersPairs(Pair.build());
             }
             
             instance.mergeClusters(controller, builder.build(), rpcCallback);
             return rpcCallback.get();
            }
           };
          Batch.Callback< mergeClustersResponse> callback = 
          new Batch.Callback<mergeClustersResponse>() {
          @Override
          public void update(byte[] region, byte[] row, mergeClustersResponse result) {
        	  	return;
              }
          };
          
          table.coprocessorService(getNeighbors.dbscanService.class, null, null, callable, callback);
          
          }
          catch(Exception e) {e.printStackTrace();}  
          catch(Throwable t) {;}
          return ;
	}
	
	private void updateNumOfNeighborsInHBase(List<getNeighbors.Data> neighbors){
		try{
			List<Put> puts = new ArrayList<Put>();
					//HTableInterface tbl = conn.getTable(this.tableName);
			HTableInterface tbl = null;
			if(this.tblConn == null){
				Configuration config = new Configuration();
				HConnection conn = HConnectionManager.createConnection(config);
				this.tblConn = conn.getTable(this.tableName);
			} 
			tbl = this.tblConn;
			int counter = 0;
			Put put = null;
			for(getNeighbors.Data Pnt: neighbors){
				counter ++;
				put = new Put(Pnt.getRowkey().getBytes());
				put.add("DBSCAN".getBytes(), "NUMOFNEIGHBORS".getBytes(), String.valueOf(Pnt.getNumOfNeighbors()).getBytes());
				puts.add(put);
				if(counter == 500){
					counter = 0;
					tbl.put(puts);
					puts.clear();
				}
			}
			if(puts.size() != 0)
				tbl.put(puts);
			//tbl.close();
		}
		catch(Exception e){e.printStackTrace();}
	}
	
	
	private void updateClusterIdInHbase(TreeMap<String, Integer> rowKeyToClusterId) throws IOException{
		
		if(rowKeyToClusterId.size() == 0) return;
		List<Put> puts = new ArrayList<Put>();
		HTableInterface tbl = null;
		if(this.tblConn == null){
			Configuration config = new Configuration();
			HConnection conn = HConnectionManager.createConnection(config);
			this.tblConn = conn.getTable(this.tableName);
		} 
		tbl = this.tblConn;
		int counter = 0;
		try{
			for(Map.Entry<String, Integer> entry: rowKeyToClusterId.entrySet()){
				Put put = new Put(entry.getKey().getBytes());
				put.addColumn("DBSCAN".getBytes(), "CLUSTERID".getBytes(), (""+entry.getValue()).getBytes());
				puts.add(put);
				counter += 1;
				if(counter == 500){
					tbl.put(puts);
					puts.clear();
					counter = 0;
				}
			}
			if(puts.size()!= 0)
				tbl.put(puts);
		}catch(Exception e){e.printStackTrace();}
	}
	
	void addPoint(String rowKey, List<Double> point) throws IOException{
		List<Put> puts = new ArrayList<Put>();
		//Configuration config = new Configuration();
		//HConnection conn = HConnectionManager.createConnection(config);
		HTableInterface tbl = null;
		if(this.tblConn == null){
			Configuration config = new Configuration();
			HConnection conn = HConnectionManager.createConnection(config);
			this.tblConn = conn.getTable(this.tableName);
		} 
		
		tbl = this.tblConn;
		String temp = "";
		String[] alp = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
		int currentAlp = 0;
		String lastQualifier = "";
		try{
			Put put = new Put(rowKey.getBytes());
			for(double value: point){
				temp = "";
				for(int i = 0; i < currentAlp/26; i ++){
					temp = temp + "d"; 
				}
				temp = temp + alp[currentAlp%26];
				currentAlp += 1;
				put.add("DATA".getBytes(), temp.getBytes(), (value + "").getBytes());
			}
			tbl.put(put);
			this.inc_dbscan(rowKey, point);
		}
		catch(Exception e){e.printStackTrace();}
	}
	
	/**
	 *Notice: This func should be called after the new point has been put into HBase
	 * @throws IOException 
	 */
	private void inc_dbscan(String rowKey, List<Double> point) throws IOException{
		ArrayList<getNeighbors.Data> UpSeeds = new ArrayList<getNeighbors.Data>();
		TreeSet<String> alreadyInUpSeeds = new TreeSet<String>();
		List<getNeighbors.Data> neighbors = getNeighbors(rowKey, this.Eps, point);
		TreeMap<String, Integer> newClusterIdMap = new TreeMap<String, Integer>(); 
		getNeighbors.Data.Builder newPntBuilder = org.dbscan.getNeighbors.Data.newBuilder();
		TreeSet<Integer> clusterSetOfUpSeeds = new TreeSet<Integer>();
		TreeMap<Integer, Integer> clustersToMerge = new TreeMap<Integer, Integer>();
		
		if(neighbors.size() >= this.Minpt){
			newPntBuilder.addAllPoint(point);
			newPntBuilder.setClusterid(0);
			newPntBuilder.setNumOfNeighbors(neighbors.size());
			newPntBuilder.setRowkey(rowKey);
			UpSeeds.add(newPntBuilder.build());
			alreadyInUpSeeds.add(rowKey);
			clusterSetOfUpSeeds.add(0);
			newClusterIdMap.put(rowKey, 0);
		}
		
		this.updateNumOfNeighbors(rowKey, neighbors);
		this.updateNumOfNeighborsInHBase(neighbors);
		
		TreeMap<String, ArrayList<getNeighbors.Data>> noiseInNeighbor = new TreeMap<String, ArrayList<getNeighbors.Data>>();
		
		for(getNeighbors.Data Pnt: neighbors){
			if(Pnt.getNumOfNeighbors() == this.Minpt && !alreadyInUpSeeds.contains(Pnt.getRowkey())){
				UpSeeds.add(Pnt);
				newClusterIdMap.put(Pnt.getRowkey(), Pnt.getClusterid());
				clusterSetOfUpSeeds.add(Pnt.getClusterid());
			}
		}
		
		int end = UpSeeds.size();
		List<getNeighbors.Data> currentNeighbors = null;
		//TreeMap<String, Integer> clusterToChange = new TreeMap<String, Integer>();
		for(int i = 0; i < end; i ++){
			if(UpSeeds.get(i).getRowkey().equals(rowKey)){
				currentNeighbors = neighbors;
			}else{
				currentNeighbors = getNeighbors(UpSeeds.get(i).getRowkey(), this.Eps, UpSeeds.get(i).getPointList());
			}
			for(getNeighbors.Data Pt: currentNeighbors){
				if(Pt.getNumOfNeighbors() >= this.Minpt && !alreadyInUpSeeds.contains(Pt.getRowkey()) ){
					UpSeeds.add(Pt);
					alreadyInUpSeeds.add(Pt.getRowkey());
					newClusterIdMap.put(Pt.getRowkey(), Pt.getClusterid());
					clusterSetOfUpSeeds.add(Pt.getClusterid());
				}
				if(Pt.getClusterid() == 0 && Pt.getNumOfNeighbors() < this.Minpt){
					
					if(!noiseInNeighbor.containsKey(UpSeeds.get(i).getRowkey())){
						noiseInNeighbor.put(UpSeeds.get(i).getRowkey(), new ArrayList<getNeighbors.Data>()); 
					}
					noiseInNeighbor.get(UpSeeds.get(i).getRowkey()).add(Pt);
					//newClusterIdMap.put(Pt.getRowkey(), Pt.getClusterid());
					//clusterToChange.put(UpSeeds.get(i).getRowkey(), Pt.getClusterid());
				}
			}
		}
		
		if(UpSeeds.size() == 0){
			//Noise
			for(getNeighbors.Data Pt: neighbors){
				if(Pt.getNumOfNeighbors() >= this.Minpt){
					newClusterIdMap.put(rowKey, Pt.getClusterid());
					break;
				}
			}
			
		}else if(clusterSetOfUpSeeds.contains(0)){
				for(int i = 0; i < UpSeeds.size(); i ++){
					if(newClusterIdMap.get(UpSeeds.get(i).getRowkey()) == 0 ){			
						newClusterIdMap.put(UpSeeds.get(i).getRowkey(), this.nextClusterId);
						for(int j = i + 1; j < UpSeeds.size(); j ++){
							if(newClusterIdMap.get(UpSeeds.get(j).getRowkey()) == 0
									&& isNeighbor(UpSeeds.get(i).getPointList(), UpSeeds.get(j).getPointList())){
								newClusterIdMap.put(UpSeeds.get(j).getRowkey(), this.nextClusterId);
							}
						}
						this.nextClusterId += 1;
					}
				}
				
				
				int clusterOfI, clusterOfJ;
				for(int i = 0; i < UpSeeds.size()-1; i ++){
					clusterOfI = newClusterIdMap.get(UpSeeds.get(i).getRowkey());
					for(int j = i+1; j < UpSeeds.size(); j ++){
						clusterOfJ = newClusterIdMap.get(UpSeeds.get(j).getRowkey());
						if( clusterOfI != clusterOfJ && isNeighbor(UpSeeds.get(i).getPointList(), UpSeeds.get(j).getPointList())){
								if(clustersToMerge.containsKey(clusterOfI)){
									clustersToMerge.put(clusterOfJ, clustersToMerge.get(clusterOfI));
									newClusterIdMap.put(UpSeeds.get(j).getRowkey(), clustersToMerge.get(clusterOfI));
								}else{
									clustersToMerge.put(clusterOfJ, clusterOfI);
									newClusterIdMap.put(UpSeeds.get(j).getRowkey(), clusterOfI);
								}
						}
					}
				}
		}else{
			//Absorption
			newClusterIdMap.put(rowKey, clusterSetOfUpSeeds.first());
		}
		
		
		for(Map.Entry<String, ArrayList<getNeighbors.Data>> entry: noiseInNeighbor.entrySet()){
			for(getNeighbors.Data Pt: entry.getValue()){
				if(!newClusterIdMap.containsKey(Pt.getRowkey())){
					newClusterIdMap.put(Pt.getRowkey(), newClusterIdMap.get(entry.getKey()));
				}
			}
		}
		
		if(newClusterIdMap.size() == 0) 
			newClusterIdMap.put(rowKey, 0);
		
		this.updateClusterIdInHbase(newClusterIdMap);
		this.mergeClusterInHbase(clustersToMerge);
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		String [] Args;
		double Eps = 1;
		int Minpt = 10;
		String fileName = "test";
		DbscanClient dbscaner = new DbscanClient(Minpt, Eps);
		if(args.length == 6){
			if(args[0].equals("--TableNonExist")){
				try{
					Eps = new Double(args[1]);
					Minpt = new Integer(args[2]);
					dbscaner = new DbscanClient(Minpt, Eps);
					dbscaner.createTable(args[3]);
					dbscaner.loadFile(args[4]);
				}
				catch(IOException e){
					System.out.println("FileName should be in abs path");
				}
				catch(Exception e){
					System.out.println("Usage1: java -jar dbscanclient.jar --TableNonExist Eps Minpt TableName fileName");
					System.out.println("Usage2: java -jar dbscanclient.jar --TableExist TableName fileName");
				}
			}
		}else if(args.length == 4 && args[0].equals("--TableExist") && dbscaner.loadParameter(args[1])){
					dbscaner.loadFile(args[2]);
		}else{
			System.out.println("Usage1: java -jar dbscanclient.jar --TableNonExist Eps Minpt TableName fileName");
			System.out.println("Usage2: java -jar dbscanclient.jar --TableExist TableName fileName");
		}
		
	}
	
}
