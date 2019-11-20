package iotsimstream;
import java.util.LinkedList;

/**
 * This class represents a channel for transmission of stream between VMs located at the same datacenter or at different datacenters.
 * It controls sharing of available bandwidth among Streams. Relation between
 * Transmission and Channel is the same as Cloudlet and CloudletScheduler,
 * but here we consider only the time shared case, representing a shared
 * channel among different simultaneous Stream transmissions.
 * 
 */
public class Channel {

	double bandwidth; //in MB/s
	private double previousTime; 
	
	LinkedList<StreamTransmission> inTransmission;
	LinkedList<StreamTransmission> completed;
	
	public Channel(double bandwidth) {
		this.bandwidth = bandwidth; /*Unit: MBps*/
		this.previousTime = 0.0;
		this.inTransmission = new LinkedList<StreamTransmission>();
		this.completed = new LinkedList<StreamTransmission>();
	}
		
	/**
	 * Updates processing of transmissions taking place in this Channel.
	 * @param currentTime current simulation time (in seconds)
	 * @return delay to next transmission completion or
	 *         Double.POSITIVE_INFINITY if there is no pending transmissions
	 */
	public double updateTransmission(double currentTime){
		double timeSpan = currentTime-this.previousTime;
		double availableBwPerHost = bandwidth/inTransmission.size();
		
		double transmissionProcessed =  timeSpan*availableBwPerHost;
		
		//update transmission and remove completed ones
		LinkedList<StreamTransmission> completedTransmissions = new LinkedList<StreamTransmission>();
		for(StreamTransmission transmission: inTransmission){
			transmission.addCompletedLength(transmissionProcessed);
			if (transmission.isCompleted()){
				completedTransmissions.add(transmission);
				this.completed.add(transmission);
			}	
		}
		this.inTransmission.removeAll(completedTransmissions);
                
		//now, predicts delay to next transmission completion
		double nextEvent = Double.POSITIVE_INFINITY;
		availableBwPerHost = bandwidth/inTransmission.size();

		for (StreamTransmission transmission:this.inTransmission){
			double eft = transmission.getLength()/availableBwPerHost;
			if (eft<nextEvent) nextEvent = eft;
		}
                
                this.previousTime=currentTime;
                
		return nextEvent;
	}
	
	/**
	 * Adds a new Transmission to be submitted via this Channel
	 * @param transmission transmission initiating
	 * @return estimated delay to complete this transmission
	 * 
	 */
	public double addTransmission(StreamTransmission transmission){
		this.inTransmission.add(transmission);
		return transmission.getLength()/(bandwidth/inTransmission.size());
	}
	
	/**
	 * Remove a transmission submitted to this Channel
	 * @param transmission to be removed
	 * 
	 */
	public void removeTransmission(StreamTransmission transmission){
		inTransmission.remove(transmission);
	}
	
	/**
	 * @return list of Streams whose transmission finished, or empty
	 *         list if no stream arrived.
	 */
	public LinkedList<StreamTransmission> getArrivedStreams(){
		LinkedList<StreamTransmission> returnList = new LinkedList<StreamTransmission>();
		
		if (!completed.isEmpty()){
			returnList.addAll(completed);
		}
		completed.removeAll(returnList);
				
		return returnList;
	}
		
	public double getLastUpdateTime(){
		return previousTime;
	}
}
