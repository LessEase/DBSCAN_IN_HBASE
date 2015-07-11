package org.dbscan.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.dbscan.getNeighbors;
import org.dbscan.getNeighbors.Data;
import org.dbscan.getNeighbors.getNeighborsRequest;
import org.dbscan.getNeighbors.getNeighborsResponse;
import org.dbscan.getNeighbors.mergeClustersRequest;
import org.dbscan.getNeighbors.mergeClustersResponse;
import org.ibm.developerworks.getRowCount;
import org.ibm.developerworks.coprocessor.RowCountEndpoint;

import com.google.common.primitives.Bytes;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class DBSCANEndpoint extends getNeighbors.dbscanService
	implements Coprocessor, CoprocessorService{
	
	  private RegionCoprocessorEnvironment env;
	  private static final Log LOG = LogFactory.getLog(DBSCANEndpoint.class);


	  public DBSCANEndpoint() {
	  }


	@Override
	public Service getService() {
		// TODO Auto-generated method stub
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment envi) throws IOException {
		// TODO Auto-generated method stub
		if(envi instanceof RegionCoprocessorEnvironment ){
			this.env = (RegionCoprocessorEnvironment) envi;
			/*RegionCoprocessorEnvironment re =(RegionCoprocessorEnvironment)envi;
		    RegionServerServices rss = re.getRegionServerServices();
		    zkw = rss.getZooKeeper();
		    try{
		    	if(ZKUtil.checkExists(zkw, zNodePath) == -1){
		    		LOG.info("hola: create znode :"+zNodePath);
		            ZKUtil.createWithParents(zkw,zNodePath);
		    	}else{
		    		  LOG.info("hola: znode exist");
		    	}
		    }
		    catch (Exception e){
		    	e.printStackTrace();
		    }*/
		}else {
		      throw new CoprocessorException("Must be loaded on a table region!");
	    }	
	}

	@Override
	public void stop(CoprocessorEnvironment arg0) throws IOException {
		// TODO Auto-generated method stub
	}
	/**
	 * param: the first point in high dimension space
	 * param: the second point in high dimension space
	 * return: distance between the two points
	 * */
	private double getDistance(List<Double> data, List<Double> temp){
		double result=0.0;	
		for(int i=0; i<data.size(); i ++){
			result += Math.pow(data.get(i)-temp.get(i), 2.0);
		}
		return Math.sqrt(result);
	}
	
	private boolean isNeighbor(double eps, List<Double> A, List<Double> B){
		return getDistance(A,B) < eps;
	}
	
	@SuppressWarnings("deprecation")
	public getNeighbors.Data buildDataFromResults(List<Cell> result){
		
		Collections.sort(result,new Comparator<Cell>(){
			public int compare(Cell A, Cell B){
				String qA = new String(A.getQualifier());
				String qB = new String(B.getQualifier());
				return qA.compareTo(qB);
			}
		});
		
		getNeighbors.Data.Builder dataBuilder = getNeighbors.Data.newBuilder();
		dataBuilder.setRowkey(new String(result.get(0).getRow()));
		for(int i = 0; i < result.size(); i ++){
			String sf = new String(result.get(i).getFamily());
			if(sf.equals("DATA")){
				dataBuilder.addPoint(new Double(new String(result.get(i).getValue())));
			}else if(sf.equals("DBSCAN")){
				String sq = new String(result.get(i).getQualifier());
				if(sq.equals("NUMOFNEIGHBORS")){
					dataBuilder.setNumOfNeighbors(new Integer(new String(result.get(i).getValue())));
				}else if(sq.equals("CLUSTERID")){
					dataBuilder.setClusterid(new Integer(new String(result.get(i).getValue())));
				}else{
					//do nothing
				}
			}
		}
		
		if(!dataBuilder.hasClusterid()){
			dataBuilder.setClusterid(0);
		}
		if(!dataBuilder.hasNumOfNeighbors()){
			dataBuilder.setNumOfNeighbors(1);
		}
		
		return dataBuilder.build();
	}
	
	@Override
	public void getNeighbours(RpcController controller,
			getNeighborsRequest request, RpcCallback<getNeighborsResponse> done) {
		// TODO Auto-generated method stub
		List<Double> data = request.getDataList();
		InternalScanner scanner = null;
		getNeighbors.getNeighborsResponse.Builder responseBuilder = getNeighbors.getNeighborsResponse.newBuilder(); 
		double Eps = request.getEps();
		try{
			Scan scan = new Scan();
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMore = false;
			int current = 0;
			getNeighbors.Data.Builder dataBuilder = getNeighbors.Data.newBuilder();
			getNeighbors.Data nd = null;
			do {
				hasMore = scanner.next(results);
				
				List<Cell> temp = new ArrayList<Cell>();
				for( ; current < results.size(); current ++){
					temp.add(results.get(current));
				}
				
				dataBuilder = dataBuilder.clear();
				nd = buildDataFromResults(temp);
				if(isNeighbor(Eps, data, nd.getPointList())){
					responseBuilder.addDatas(nd);
				}

	        }while (hasMore);
		}catch(IOException e){
		}
		
		finally {
	        if (scanner != null) {
	          try {
	            scanner.close();
	          } catch (IOException ignored) {}
	        }
	      }
	    done.run(responseBuilder.build());
	}


	@Override
	public void mergeClusters(RpcController controller,
			mergeClustersRequest request,
			RpcCallback<mergeClustersResponse> done) {
		
		TreeMap<Integer, Integer> clustersToMerge = new TreeMap<Integer, Integer>();
		for(getNeighbors.clustersPair pair: request.getClustersPairsList()){
			clustersToMerge.put(pair.getCluster1(), pair.getCluster2());
		}
		
		InternalScanner scanner = null;
		getNeighbors.mergeClustersResponse.Builder responseBuilder = getNeighbors.mergeClustersResponse.newBuilder();
		responseBuilder.setSucceed(true);
		Put put = null;
		List<Put> puts = new ArrayList<Put>();
		int counter = 0;
		try{
			Scan scan = new Scan();
			scanner = env.getRegion().getScanner(scan);
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMore = false;
			int current = 0;
			getNeighbors.Data.Builder dataBuilder = getNeighbors.Data.newBuilder();
			getNeighbors.Data nd = null;
			int cluster1, cluster2;
			String sf = null, sq = null;
			do {
				
				hasMore = scanner.next(results);
				for( ;current < results.size(); current ++){
					sf = new String(results.get(current).getFamily());
					sq = new String(results.get(current).getQualifier());
					if(sf.equals("DBSCAN") && sq.equals("CLUSTERID")){
						cluster1 = new Integer(new String(results.get(current).getValue()));
						if(clustersToMerge.containsKey(cluster1)){
							put = new Put(results.get(current).getRow());
							String value = "" + clustersToMerge.get(cluster1);
							put.add(results.get(current).getFamily(), results.get(current).getQualifier(), value.getBytes());
							env.getRegion().put(put);
						}
					}
				}

	        }while (hasMore);
	     }
		catch (IOException e){;}
		finally {
	        if (scanner != null) {
	          try {
	            scanner.close();
	          } catch (IOException ignored) {}
	        }
	      }
	    done.run(responseBuilder.build());
	}

}
