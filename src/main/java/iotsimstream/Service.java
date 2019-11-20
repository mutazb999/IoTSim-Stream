package iotsimstream;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * This class encapsulates a DAG Node (i.e. Service). The service contains its data processing requirement, 
 * its user-defined requirement, the actual Cloudlet(s) that will be continuously running during graph application execution, 
 * information about its presents, children and stream dependencies.
 * 
 * @author Mutaz Barika
 */
public class Service {
        int serviceID;
        ArrayList<ServiceCloudlet> cloudlets;
        List<Stream> streamDependencies;
	List<Stream> output;
        double dpRequiement; // service data processing requriement (service requirement) - milion instructions per MB
        double userdprateRequiement; // user data processing rate req. - MB/s
        List<Service> parents;
	List<Service> children;
	
        public Service(int serviceID, int ownerId, double dpReq, double userdprateReq){
	        this.serviceID=serviceID;
                cloudlets=new ArrayList<>();
		parents = new LinkedList<Service>();
		children = new LinkedList<Service>();
		streamDependencies = new LinkedList<Stream>();
		output = new LinkedList<Stream>();
	        this.dpRequiement=dpReq;
                this.userdprateRequiement=userdprateReq;
        }
	
	public int getId(){
            return serviceID;
	}

    public double getDataProcessingReq() {
        return dpRequiement;
    }

    public double getUserDataProcessingRateReq() {
        return userdprateRequiement;
    }
	
    public void addCloudlet(ServiceCloudlet scl)
    {
        cloudlets.add(scl);
    }
    
	public void addParent(Service parent){
		parents.add(parent);
	}
	
	public List<Service> getParents(){
		return parents;
	}
	
	public void addChild(Service parent){
		children.add(parent);
	}
	
	public List<Service> getChildren(){
		return children;
	}
	
	public void addStreamDependency(Stream data){
		streamDependencies.add(data);
	}
	
	public List<Stream> getStreamDependencies(){
		return streamDependencies;
	}
	
	public ServiceCloudlet getServiceCloudletByVM(int vmid){
            for(ServiceCloudlet cl: cloudlets)
            {
                if(cl.getVmId()==vmid)
                    return cl;
            }
            return null;
	}
	
	public void addOutput(Stream stream){
		output.add(stream);
	}
	
	public List<Stream> getOutput(){
		return output;
	}
        
        public ArrayList<ServiceCloudlet> getServiceCloudlets()
        {
            return cloudlets;
        }
        
        public ServiceCloudlet getServiceCloudletByID(int cloudletID)
        {
            ServiceCloudlet retunCloudlet=null;
            
            for(ServiceCloudlet cloudlet: cloudlets)
                if(cloudlet.getCloudletId()==cloudletID)
                {
                    retunCloudlet=cloudlet;
                    break;
                }
            
            return retunCloudlet;
        }
		
	public void setVmId(int cloudletID, int vmId){
		//if(cloudlet!=null) cloudlet.setVmId(vmId);
                ServiceCloudlet cl=cloudlets.get(cloudletID);
                if(cl!=null)
                    cl.setVmId(vmId);
	}
	
	public int getCloudletVmId(int cloudletID){
		return cloudlets.get(cloudletID).getVmId();
	}
	
        public double getTotalSizeOfServiceInputStreams()
        {
            double totalSize=0.0;

            for(Stream inStream: getStreamDependencies())
            {
                if(isStreamProducerEXSource(inStream))
                    totalSize+=inStream.getSize();
                else //producer is service
                {
                    if(inStream.getReplicaProcessing().contains(serviceID))
                    {
                        //Replica processing
                        totalSize+=inStream.getSize();
                    }
                    else if(inStream.getPartitionProcessing().containsKey(serviceID))
                    {
                        //Partition processing
                        double partitionPercentage=inStream.getPartitionProcessing().get(serviceID);
                        totalSize+=inStream.getSize()* (partitionPercentage/100);
                    }
                }
            }
            return totalSize;
        }
        
        public boolean isStreamProducerEXSource(Stream stream)
        {
            if(stream.getTypeOfProducer().equalsIgnoreCase("exsource"))
                return true;
            else
                return false;
        }
}
