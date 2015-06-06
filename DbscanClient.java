package org.dbscan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.dbscan.getNeighbors;

public class DbscanClient {

	private int Minpt = 4;
	private double Eps = 4.0;
	private int nextClusterId = 1;
	
	public void createTable(String tableName){
		try{
	        Configuration config = new Configuration();
	        HBaseAdmin admin = new HBaseAdmin(config);
	        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
	        if(admin.tableExists(tableName) == true) {
	          admin.disableTable(tableName);
	          admin.deleteTable(tableName);
	        }
	        tableDesc.addFamily(new HColumnDescriptor("DATA")); //add column family
	        tableDesc.addFamily(new HColumnDescriptor("DBSCAN"));
	        //tableDesc.addCoprocessor("org.dbscan.coprocessor.RowCountObserver");
	        tableDesc.addCoprocessor("org.dbscan.coprocessor.DBSCANEndpoint");
	        admin.createTable(tableDesc);
	      }
	      catch(Exception e) {e.printStackTrace();}
	}
	
	/*public List<String> getNeighbors(String tableName, String data){
		 List<String> neighbors = null;
		try{
	        Configuration config = new Configuration();
	        HConnection conn = HConnectionManager.createConnection(config);
	        HTableInterface tbl = conn.getTable(tableName);
	      CoprocessorRpcChannel channel = tbl.coprocessorService("2".getBytes()); 
	      org.dbscan.getNeighbors.dbscanService.BlockingInterface service = org.dbscan.getNeighbors.dbscanService.newBlockingStub(channel);
	      org.dbscan.getNeighbors.getNeighborsRequest.Builder request = org.dbscan.getNeighbors.getNeighborsRequest.newBuilder();
	      request.setData(data);
	      org.dbscan.getNeighbors.getNeighborsResponse ret = service.getNeighbours(null, request.build());
	      neighbors = ret.getDatasList();
	   
	     }
	     catch(Exception e) {e.printStackTrace();}
		   
		return neighbors;	
	}*/
	
	public boolean isNeighbor(List<Double> A, List<Double> B){
		//To do
		return true;
	}
	
	public List<getNeighbors.Data> getNeighbors(String rowKey, List<Double> point){
		//To Do
		return null;
	}
	
	public void updateNumOfNeighbors(List<getNeighbors.Data> re){
		//To Do
	}
	
	public void updateDatabase(){
		
	}
	
	public void expand(TreeMap<String, ArrayList<getNeighbors.Data>> noiseInNeighbor){
		
	}
	
	/**
	 *Pre: This process should be called after the new point has been put into HBase
	 */
	void inc_dbscan(String rowKey, ArrayList<Double> point){
		ArrayList<getNeighbors.Data> UpSeeds = new ArrayList<getNeighbors.Data>();
		List<getNeighbors.Data> neighbors = getNeighbors(rowKey, point);
		TreeMap<String, Integer> newClusterIdMap = new TreeMap<String, Integer>(); 
		getNeighbors.Data.Builder newPntBuilder = org.dbscan.getNeighbors.Data.newBuilder();
		if(neighbors.size() >= this.Minpt){
			
			newPntBuilder.addAllPoint(point);
			newPntBuilder.setClusterid(0);
			newPntBuilder.setNumOfNeighbors(neighbors.size()+1);
			newPntBuilder.setRowkey(rowKey);
			UpSeeds.add(newPntBuilder.build());
			alreadyInUpSeeds.add(rowKey);
			newClusterIdMap.put(UpSeeds.get(0).getRowkey(), UpSeeds.get(0).getClusterid());
		}
		
		updateNumOfNeighbors(neighbors);
		TreeSet<Integer> clusterSetOfUpSeeds = new TreeSet<Integer>();
		TreeMap<String, ArrayList<getNeighbors.Data>> noiseInNeighbor = new TreeMap<String, ArrayList<getNeighbors.Data>>();
		
		for(getNeighbors.Data Pnt: neighbors){
			if(Pnt.getNumOfNeighbors() == this.Minpt){
				UpSeeds.add(Pnt);
				newClusterIdMap.put(Pnt.getRowkey(), Pnt.getClusterid());
				clusterSetOfUpSeeds.add(Pnt.getClusterid());
			}
		}
		
		int end = UpSeeds.size();
		for(int i = 0; i < end; i ++){
			List<getNeighbors.Data> currentNeighbors = null;
			if(i == 0){
				currentNeighbors = neighbors;
			}else{
				currentNeighbors = getNeighbors(UpSeeds.get(i).getRowkey(), UpSeeds.get(i).getPointList());
			}
			for(getNeighbors.Data Pt: currentNeighbors){
				if(Pt.getNumOfNeighbors() >= this.Minpt && !alreadyInUpSeeds.contains(Pt.getRowkey()) ){
					UpSeeds.add(Pt);
					alreadyInUpSeeds.add(Pt.getRowkey());
					newClusterIdMap.put(Pt.getRowkey(), Pt.getClusterid());
					clusterSetOfUpSeeds.add(Pt.getClusterid());
				}
				if(Pt.getClusterid() == 0){
					if(!noiseInNeighbor.containsKey(UpSeeds.get(i).getRowkey())){
						noiseInNeighbor.put(UpSeeds.get(i).getRowkey(), new ArrayList<getNeighbors.Data>()); 
					}
					noiseInNeighbor.get(UpSeeds.get(i).getRowkey()).add(Pt);
					newClusterIdMap.put(Pt.getRowkey(), Pt.getClusterid());
				}
			}
		}
		
		
		if(UpSeeds.size() == 0){
			for(getNeighbors.Data Pt: neighbors){
				if(Pt.getClusterid() != 0){
					newClusterIdMap.put(rowKey, Pt.getClusterid());
				}
			}
			
		}else if(clusterSetOfUpSeeds.size() == 1){
			
			if(clusterSetOfUpSeeds.first() == 0){
				ArrayList<Integer> visited = new ArrayList<Integer>();
				for(int i = 0; i < UpSeeds.size(); i ++){
					visited.add(0);
				}
				
				for(int i = 0; i < UpSeeds.size(); i ++){
					if(visited.get(i) == 0){
						visited.set(i, 1);
						newClusterIdMap.put(UpSeeds.get(i).getRowkey(), this.nextClusterId);
						for(int j = i + 1; j < UpSeeds.size(); j ++){
							if(isNeighbor(UpSeeds.get(i).getPointList(), UpSeeds.get(j).getPointList())){
								visited.set(j, 1);
								newClusterIdMap.put(UpSeeds.get(j).getRowkey(), this.nextClusterId);
							}
						}
						this.nextClusterId += 1;
					}
				}
			}else{
					
				//Absorption
			}
		}else{
			//Merge
			if(clusterSetOfUpSeeds.contains(0)){
				for(int i = 0; i < UpSeeds.size(); i ++){
					if(newClusterIdMap.get(UpSeeds.get(i).getRowkey()) == 0){
						newClusterIdMap.put(UpSeeds.get(i).getRowkey(), this.nextClusterId);
						for(int j = i + 1; j < UpSeeds.size(); j ++){
							if(newClusterIdMap.get(UpSeeds.get(j).getRowkey()) == 0
									&& isNeighbor(UpSeeds.get(i).getPointList(), UpSeeds.get(j).getPointList())){
								newClusterIdMap.put(UpSeeds.get(i).getRowkey(), this.nextClusterId);
							}
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
						newClusterIdMap.put(UpSeeds.get(j).getRowkey(), clusterOfI);
					}
				}
			}
		}
		
		/*
		 * 
		int clusterId;
		for(Map.Entry<String, ArrayList<getNeighbors.Data>> entry: noiseInNeighbor.entrySet()){
			for(getNeighbors.Data item: entry.getValue()){
				clusterId = item.getClusterid();
				if(newClusterIdMap.containsKey(item.getRowkey())){
					clusterId = newClusterIdMap.get(item.getRowkey());
				}
				if(clusterId == 0){
					
				}
			}
		}
		*/
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
