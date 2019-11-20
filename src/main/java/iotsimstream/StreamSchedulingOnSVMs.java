package iotsimstream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;

/** It is a class designed to schedule the divided portions of each stream either
 * input or output stream on SVMs of destination service according to their computing power.
 *
 * @author Mutaz Barika
 */
public class StreamSchedulingOnSVMs {
    //This class will be used by any other object to query about datacenter canonical ids, vm and datacenter map and required locations (VMs) for any stream; For example, Datacenter to query about the mapping of VMs and Datacenters as well as the required locations for given stream
    
    final static double minDPUnit=GraphAppEngine.minDPUnit;//=Integer.parseInt(Properties.DATA_PROCESSING_MINUNIT.getProperty());
    static Hashtable<Integer,HashSet<Integer>> streamRequiredLocation; //each entry is stream id and list of destination VMs IDs where such stream should be avaliable
    static HashMap<Integer,Integer> ProSVMDatacenterIDsMap; //each entry is VM ID and corresponding datacenter ID
    static HashMap<Integer, Integer> CanonicalIncrementalDatacenterIDsMap;
    static ArrayList<Service> services;
    static Hashtable<Integer,SVM> vmTable;
    
    static HashMap<String,Integer> workingStreamClouldetIDsIncrementNumberMap;
    static HashMap<String,Integer> workingServiceStreamIDsCountMap;
    static HashMap<Integer,ArrayList<Integer>> serviceCloudletsRoundRobinValues;
    static double totalStreamsTransffered=0.0;
    
    public static void init() {
        streamRequiredLocation=new Hashtable<>();
        ProSVMDatacenterIDsMap=new HashMap<>();
        CanonicalIncrementalDatacenterIDsMap=new HashMap<>();
        services=new ArrayList<>();
        vmTable=new Hashtable<>();
        workingStreamClouldetIDsIncrementNumberMap=new HashMap<>();
        workingServiceStreamIDsCountMap=new HashMap<>();
        serviceCloudletsRoundRobinValues=new HashMap<>();
        totalStreamsTransffered=0.0;
    }
    
    public static void setStreamRequiredLocation(Hashtable<Integer,HashSet<Integer>> streamRequiredLocation) {
        StreamSchedulingOnSVMs.streamRequiredLocation=streamRequiredLocation;
    }
    
    public static void setProSVMDatacenterIDsMap(HashMap<Integer,Integer> ProSVMDatacenterMap) {
        StreamSchedulingOnSVMs.ProSVMDatacenterIDsMap=ProSVMDatacenterMap;
    }
    
    public static void setDatacenterCanonicalIDsMap(HashMap<Integer, Integer> DatacenterCanonicalIDsMap) {
        StreamSchedulingOnSVMs.CanonicalIncrementalDatacenterIDsMap=DatacenterCanonicalIDsMap;
    }
    
    public static void setServices(ArrayList<Service> services) {
        StreamSchedulingOnSVMs.services=services;
    }
    
    public static void setVMTable(Hashtable<Integer,SVM> vmTable) {
        StreamSchedulingOnSVMs.vmTable=new Hashtable<>(vmTable);
    }
    
    public static int getDatacenterID(int VMid)
    {
        return ProSVMDatacenterIDsMap.get(VMid);
    }
    
    public static HashMap<Integer,Integer> getDestVMIDDatacenterIDMapForStream(int streamID)
    {
        //Get required locations for this stream
        HashSet<Integer> locationsForOutputStream= streamRequiredLocation.get(streamID); //detination VMs for this stream
        
        //Prepare destination VMDatacenter map for this stream, where each entry contains destination VM with corrsponding destination datacenter
        HashMap<Integer,Integer> map=new HashMap<Integer,Integer>(); //Each entry is destination VM with corssponding datcenter
        for(Integer destvmid:locationsForOutputStream)
        {
            int destDatacenterID=ProSVMDatacenterIDsMap.get(destvmid);
            map.put(destvmid, destDatacenterID); //destination VM with datacenter
        }
        
        return map;
    }
    
    /* This method to get the list of stream portions of given stream and the information of BigDatacenters and
     * SVMs where these portions should be transferred and available.
     * This method ia working with both stream produced by external source or service
    */
    public static LinkedHashMap<Stream, Integer> getStreamPortionsSchedulingOnVMs(Stream stream)
    {
        LinkedHashMap<Stream, Integer> streamPortionsVMMap= new LinkedHashMap<>(); //Each entry is stream portion and vmid in which such stream will be scheduled on the VM  //using LinkedHashMap in order to preserver the order of adding stream portions in the map, so that they will be sent according to their order of addition
        
        //service ids for which this stream shoulb be available
        ArrayList<Integer> list = new ArrayList<>(streamRequiredLocation.get(stream.getId())); 
        if(list.size()>0) //this list contains service ids in which this stream should be available on them
        {
            System.out.println("------------------- Strart of Stream#" + stream.getId() + " Partition -------------------");
            
            //Get original stream size and check if it is under miniDPUnit, if so it will be rounded to minDPUnit
            double streamSize=atLeastMinDPRate(stream);
            
            for(Integer serviceID: list)
            {
                int portionID=0;
                String k=serviceID + "_" + stream.getId();
                if(!workingServiceStreamIDsCountMap.containsKey(k))
                    workingServiceStreamIDsCountMap.put(k, 0);
                else
                    portionID=workingServiceStreamIDsCountMap.get(k);
                
                //Get round-robin values for cloudlets of this service
                ArrayList<Integer> ServiceCloudletsRRValues= serviceCloudletsRoundRobinValues.get(serviceID);
                
                System.out.print("--For Service#" + serviceID +", ");
                
                //Get Service
                Service service=services.get(serviceID);

                //Determine the processing type of stream for this service, meaning that replica or partition percentage
                double streamSizeTowardsService = 0.0;
                if(stream.getReplicaProcessing().contains(serviceID))
                {
                    //Replica processing
                    streamSizeTowardsService=streamSize;
                    System.out.print("replica copy\n");
                }
                else if(stream.getPartitionProcessing().containsKey(serviceID))
                {
                    //Partition processing
                    double partitionPercentage=stream.getPartitionProcessing().get(serviceID);
                    streamSizeTowardsService= streamSize * (partitionPercentage/100);
                    
                    System.out.print("Partition Copy("+ partitionPercentage + "%) from ");
                    
                    
                    //Check if stream size after parition is less than minDPUnit - should be chekced as after paritioing could size lower than minimum unit
                    if(streamSizeTowardsService<minDPUnit)
                    {
                        streamSizeTowardsService=minDPUnit;
                        System.out.print("- Note: stream size after partition is less than miniDPUnit so that minDPUnit is considered\n");
                    }
                }
                else //stream from external source
                    streamSizeTowardsService=streamSize;

                //Get number of portions based on miniDPUnit for stream and the reamin size
                int numOfMinPortions=(int) (streamSizeTowardsService / minDPUnit);
                
                double remainSize= streamSizeTowardsService - (minDPUnit * numOfMinPortions);
                
                //Reamin size at least minDPRate if not zero
                if(remainSize>0)
                    remainSize=atLeastMinDPRate(remainSize, stream.getId());
                
                
                //Just for printing
                if(remainSize==0)
                    System.out.println("--- Original Stream#" + stream.getId() + ": Size:" + streamSize+ " Number of Portions:" + numOfMinPortions);
                else
                    System.out.println("--- Original Stream#" + stream.getId() + ": Size:" + streamSize+ " Rounded to: " + ((numOfMinPortions*minDPUnit)+remainSize) + " Number of Portions:" + (numOfMinPortions + 1));

                /*Assign Stream portions to cloudlets based on their capacity/power in cyclic manner, exclulding remaning size from parition which will be assigned 
                  later on (see next code), where the previous knowledge (workingStreamClouldetIDsIncrementNumberMap) will be used to continoue from the previous assignemnt
                  in order to ensure the balance of portions among service cloudlets
                */
                
                //Get index of cloudlet which its capacity is less than max or it is not even assigned a stream as it is not in workingStreamClouldetIDsIncrementNumberMap
                int index=-1; //current cloudlet index
                for(int ind=0;ind<service.getServiceCloudlets().size();ind++)
                {
                    String comKey=stream.getId() + "_" + service.getServiceCloudlets().get(ind).getCloudletId(); 
                    if( (workingStreamClouldetIDsIncrementNumberMap.containsKey(comKey) 
                            && workingStreamClouldetIDsIncrementNumberMap.get(comKey)<ServiceCloudletsRRValues.get(ind) )
                         ||  !workingStreamClouldetIDsIncrementNumberMap.containsKey(comKey)  
                       )
                    {
                            index=ind;
                            break;
                    }
                }
                if(index==-1)  //all couldlets are in full capacity, so set the increment values (number of assigned portions to zero
                {
                    for(ServiceCloudlet cl: service.getServiceCloudlets())
                    {
                        String key= stream.getId() + "_" + cl.getCloudletId();
                        workingStreamClouldetIDsIncrementNumberMap.put(key,0);
                    }
                    index=0; //start from first cloudlet
                }
                
                ServiceCloudlet cloudlet=service.getServiceCloudlets().get(index); //get first cloudlet of this service
                
                for(int i=0;i<numOfMinPortions;i++)
                {
                    Stream streamPortion= new Stream(stream.getId(), stream.getOwnerId(), stream.getProducerid(), stream.getTypeOfProducer(), minDPUnit);
                    streamPortion.setReplicaProcessing(stream.getReplicaProcessing());
                    streamPortion.setPartitionProcessing(stream.getPartitionProcessing());
                    streamPortion.setIsPortion(true);
                    streamPortion.setPortionID(portionID);
                    streamPortion.setStreamTime(stream.getStreamTime());
                    
                    //The key is stream id + underscore + cloudlet id; this key is used to retrieve incremental count that being used for assignemnt portion to approperiate cloudlet (i.e. start from or from the appropriate cloudlet according to capacity and previous assignemnt knowledge)
                    String compositeKey=stream.getId() + "_" + cloudlet.getCloudletId();
                    if(!workingStreamClouldetIDsIncrementNumberMap.containsKey(compositeKey))
                        workingStreamClouldetIDsIncrementNumberMap.put(compositeKey, 0);
                    
                    //Get increment number for the stream and cloudlet composition that indicates the number of this stream portions assigned to this cloudlet from previous or current portions; for example, if the previous portions of this stream is coming, the number is count those portions to allow start assignemnt from the previously step in order to balance the assignemt of all portions of this stream from different calls among service cloudlets 
                    int StreamCloudletIncrementNum=workingStreamClouldetIDsIncrementNumberMap.get(compositeKey);
                    
                    if(StreamCloudletIncrementNum < ServiceCloudletsRRValues.get(index)) // 
                    {
                        StreamCloudletIncrementNum++;
                        
                        workingStreamClouldetIDsIncrementNumberMap.put(compositeKey,StreamCloudletIncrementNum);
                        streamPortionsVMMap.put(streamPortion, cloudlet.getVmId());
                        
                        System.out.println("\tStream#"+ stream.getId() +" Portion#" + portionID + " Size: " + streamPortion.getSize() + ": is assigned to ServiceCloudlet#" +cloudlet.getCloudletId() );
                        
                        portionID++;
                    }
                    else //the current cloudlet reachs its capacity so that the next service cloudlet will be used
                    {
                        //Check if all cloudlets of this service reach their max round-robin values in number of stream portions in one cycle so that the next cycle should begin; For that all increment number for these cloudlets are set to zero again
                        boolean check=true;
                        int newIndex=-1;
                        for(int ind=0;ind<service.getServiceCloudlets().size();ind++)
                        {
                            String comKey=stream.getId() + "_" + service.getServiceCloudlets().get(ind).getCloudletId(); 
                            if( (workingStreamClouldetIDsIncrementNumberMap.containsKey(comKey) 
                                    && workingStreamClouldetIDsIncrementNumberMap.get(comKey)<ServiceCloudletsRRValues.get(ind) )
                                 ||  !workingStreamClouldetIDsIncrementNumberMap.containsKey(comKey)  
                               )
                            {
                                    check=false;
                                    newIndex=ind;
                                    break;
                            }
                        }
                        if(check) //or if(index==service.getServiceCloudlets().size()-1) //last service cloudlet
                        {
                            for(ServiceCloudlet cl: service.getServiceCloudlets())
                            {
                                String key= stream.getId() + "_" + cl.getCloudletId();
                                workingStreamClouldetIDsIncrementNumberMap.put(key,0);
                            }
                            newIndex=0;
                        }
                        
                        //Go to next cloudlet
                        index=newIndex; //jump to the next available cloudlet
                        cloudlet=service.getServiceCloudlets().get(index);
                        
                        //Go backward to assign this portion becuase it is still not assigned
                        i--;
                    }
                }
                   
                //Assign remaning size of original stream to approperiate cloudlet (we start from cloudlet used above)
                if(remainSize>0)
                {
                    while(remainSize!=0)
                    {
                        Stream streamPortion= new Stream(stream.getId(), stream.getOwnerId(), stream.getProducerid(), stream.getTypeOfProducer(), remainSize);
                        streamPortion.setReplicaProcessing(stream.getReplicaProcessing());
                        streamPortion.setPartitionProcessing(stream.getPartitionProcessing());
                        streamPortion.setIsPortion(true);
                        streamPortion.setPortionID(portionID);
                        streamPortion.setStreamTime(stream.getStreamTime());


                        String compositeKey=stream.getId() + "_" + cloudlet.getCloudletId();
                        if(!workingStreamClouldetIDsIncrementNumberMap.containsKey(compositeKey))
                            workingStreamClouldetIDsIncrementNumberMap.put(compositeKey, 0);

                        int incrementNum=workingStreamClouldetIDsIncrementNumberMap.get(compositeKey);
                        
                        if(incrementNum < ServiceCloudletsRRValues.get(index))
                        {
                            incrementNum++;
                            workingStreamClouldetIDsIncrementNumberMap.put(compositeKey,incrementNum);
                        
                            streamPortionsVMMap.put(streamPortion, cloudlet.getVmId());

                            System.out.println("\tStream#" + stream.getId() + " Portion#" + portionID + " Size: " + streamPortion.getSize() + ": is assigned to ServiceCloudlet#" +cloudlet.getCloudletId() );
                            remainSize=0;
                            portionID++;
                        }
                        else
                        {
                            //Check if all cloudlets of this service reach their max round-robin values in number of stream portions in one cycle so that the next cycle should begin; For that all increment number for these cloudlets are set to zero again
                            boolean check=true;
                            for(int ind=0;ind<service.getServiceCloudlets().size();ind++)
                            {
                                String comKey=stream.getId() + "_" + service.getServiceCloudlets().get(ind).getCloudletId(); 
                                if(workingStreamClouldetIDsIncrementNumberMap.containsKey(comKey) 
                                        && workingStreamClouldetIDsIncrementNumberMap.get(comKey)<ServiceCloudletsRRValues.get(ind))
                                        check=false;
                            }
                            if(check) //or if(index==service.getServiceCloudlets().size()-1) //last service cloudlet
                            {
                                for(ServiceCloudlet cl: service.getServiceCloudlets())
                                {
                                    String key= stream.getId() + "_" + cl.getCloudletId();
                                    workingStreamClouldetIDsIncrementNumberMap.put(key,0);
                                }
                            }

                            index=(index+1) % service.getServiceCloudlets().size();
                            cloudlet=service.getServiceCloudlets().get(index);
                        }

                        
                    }
                }
                workingServiceStreamIDsCountMap.put(serviceID + "_" + stream.getId(), portionID);
            }
            System.out.println("------------------- End of Stream#" + stream.getId() + " Partition-------------------");
        }
        
        //Add the total size of these stream portions to totalStreamsTransffered
        for(Stream streamportion:streamPortionsVMMap.keySet())
            totalStreamsTransffered+=streamportion.getSize();

        return streamPortionsVMMap;
    }    
    
    public static void computeRRValuesForServicesCloudlets()
    {
        //Compute roud-robin values for ServiceCloudlets of each service
        for(int i=0;i<services.size();i++)
        {
            Service service=services.get(i);
            ArrayList<Integer> values=new ArrayList<>();
            
            //find the minimum computing power among ServiceCloudlets of this servoce
            double minCloudletMIPS=Double.MAX_VALUE;
            for(ServiceCloudlet cloudlet: service.getServiceCloudlets())
                if(getVMTotoalMIPS(cloudlet.getVmId())<minCloudletMIPS)
                    minCloudletMIPS=getVMTotoalMIPS(cloudlet.getVmId());
            
            //Get round-robin value for each ServiceCloudlet to stream portions scheduling
            for(ServiceCloudlet cloudlet: service.getServiceCloudlets())
            {
                int value= (int) Math.floor(getVMTotoalMIPS(cloudlet.getVmId())/minCloudletMIPS);
                values.add(value);
            }

            serviceCloudletsRoundRobinValues.put(service.getId(), values);
        }
    }
    
    
    
    //This MIPS is the total MIPS = MIPS * Number of PEs
    public static double getVMTotoalMIPS(int vmid)
    {
        return vmTable.get(vmid).getCurrentRequestedTotalMips();
    }
    
    private static double atLeastMinDPRate(Stream stream)
    {
        double  roundedSize=stream.getSize();
        if(roundedSize<minDPUnit)
        {
            roundedSize=minDPUnit;
            System.out.println("-Note: The size of original stream#" + stream.getId() + " is " + stream.getSize() + " (less than miniDPUnit),  Rounded to: " +roundedSize);
        }
        return roundedSize;
    }
    
    private static double atLeastMinDPRate(double remainStreamSize, int originalStreamID)
    {   
        //originalStreamID parameter is only used for printing purpose
        
        double  roundedSize=remainStreamSize;
        if(roundedSize<minDPUnit)
        {
            roundedSize=minDPUnit;
            System.out.println("-Note: The size of remain portion for stream#" + originalStreamID + " is " + remainStreamSize + " (less than miniDPUnit),  Rounded to: " + roundedSize);
        }
        return roundedSize;
    }
}
