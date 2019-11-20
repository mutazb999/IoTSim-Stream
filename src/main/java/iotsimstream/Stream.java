package iotsimstream;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Hashtable;
import org.cloudbus.cloudsim.core.CloudSim;

/**
 *
 * @author Mutaz Barika
 */
public class Stream implements Comparable<Stream>{
    
    int id;
    int ownerId;
    String typeOfProducer;
    int producerid;
    double size; //for simulation purpose, it is in MB/s
    HashSet<Integer> locations; //
    boolean isPortion;
    boolean isCompoundPortion; //it is stream portion that combines multiple original stream portions
    int portionID;
    
    HashSet<Integer> ReplicaProcessing; //each entry is serviceID, where such service processes the replica of this stream  
    Hashtable<Integer, Double> PartitionProcessing; //each entry is the serviceID and partition percentage; That's meaning this service will process this percentage of stream
    
    double stream_time; //Generate Time in Second
    double stream_arrival_time; //Queue Arrival Time in Second (time for stream entering the input queue)
    
    
    public Stream(int id, int ownerId, int producerid, String typeOfProducer, double size) {
        this.id = id;
        this.ownerId=ownerId;
        this.producerid = producerid;
        this.size = size;
        locations= new HashSet<>();
        ReplicaProcessing=new HashSet<>();
        PartitionProcessing=new Hashtable<>();
        this.typeOfProducer=typeOfProducer;
        this.portionID=-1; //means stream is not paritioned by default, if so the value has been changed later
        this.isCompoundPortion=false;
        this.stream_time = CloudSim.clock();
        this.stream_arrival_time=0.0;
    }

    public int getId() {
        return id;
    }

    public double getSize() {
        return size;
    }

    public String getTypeOfProducer() //"exsource" or "service"
    {
        return typeOfProducer;
    }
    
    public int getProducerid() {
        return producerid;
    }

    public int getOwnerId() {
        return ownerId;
    }

    public void setId(int id) {
        this.id = id;
    }
    
    public void addLocation(int locationId){
            locations.add(locationId);
    }

    public void removeLocation(int locationId){
            locations.remove(locationId);
    }

    public boolean isAvailableAt(int locationId){
            return (locations.contains(locationId));
    }
    public void removeAllLocation(){
            locations= new HashSet<>();
    }

    public void setReplicaProcessing(HashSet<Integer> ReplicaProcessing) {
        this.ReplicaProcessing = ReplicaProcessing;
    }

    public void setPartitionProcessing(Hashtable<Integer, Double> PartitionProcessing) {
        this.PartitionProcessing = PartitionProcessing;
    }
    
        
    public void addReplicaProcessing(int serviceID)
    {
        ReplicaProcessing.add(serviceID);
    }
    
    public void addPartitionProcessing(int serviceID, double percent)
    {
        PartitionProcessing.put(serviceID , percent);
    }
    
    public HashSet<Integer> getReplicaProcessing() {
        return ReplicaProcessing;
    }

    public Hashtable<Integer, Double> getPartitionProcessing() {
        return PartitionProcessing;
    }
    
    public boolean isPortion() {
        return isPortion;
    }

    public int getPortionID() {
        return portionID;
    }
    
    
    public void setIsPortion(boolean isPortion) {
        this.isPortion = isPortion;
    }

    public void setPortionID(int portionID) {
        this.portionID = portionID;
    }

    public boolean isCompoundPortion() {
        return isCompoundPortion;
    }

    public void setIsCompoundPortion(boolean isCompoundPortion) {
        this.isCompoundPortion = isCompoundPortion;
    }

    public void setStreamTime(double time)
    {
        stream_time = time;
    }
    
    public double getStreamTime()
    {
        return stream_time;
    }
    
    public void setArrivalTime(double time)
    {
        stream_arrival_time = time;
    }
   
    public double getStreamArrivalTime()
    {
        return stream_arrival_time;
    }
    
    public void setSize(double size) {
        this.size = size;
    }
    
    
    @Override
    public int compareTo(Stream o){
        return Comparator.comparing(Stream::getPortionID)
              .thenComparing(Stream::getId)
              .compare(this, o);
    }
    
}
