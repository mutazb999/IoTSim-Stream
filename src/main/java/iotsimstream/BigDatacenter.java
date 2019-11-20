package iotsimstream;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;


/**
 * This class extends data center to support simulation
 * of stream graph application including internal and external data
 * transfer delays if data has to be moved between VMs in datacenter
 * or to VM in another datacenter.
 */
public class BigDatacenter extends Datacenter {
	
	public static final int UPDATE_NETWORK = 455671;
	public static final int TRANSFER_STREAM = 455672;
	public static final int STREAM_AVAILABLE = 455673;
        public static final int EXSOURCE_STREAM  = 222222;
        
	
	//billing interval in minutes
	//public static final long BILLING_INTERVAL = 60; 
	
        //defines minimum quantum of time between events in seconds. 
	protected static final double QUANTUM = 0.01; //It affects accuracy of the simulation
	
	Hashtable<Integer,SVM> vmTable;
	Hashtable<Long,Channel> vmChannelTable;
        Hashtable<Long,Channel> destDatacenterChannelTable;
	Hashtable<SVM,Long> vmCreationTime;
	Hashtable<SVM,Double> vmPrice;
	
	Random random;
	long basicCpuUnit;
	double ingressBandwidth;
	double ingressLatency;
        HashMap<Integer, Double> destDatacenterEgressBwMap; //This map is for bandwidth values of this datacenter with other datacetners. Each entry is destination datacenter id and egress bandwidth value.
        HashMap<Integer, Double> destDatacenterEgressLatMap; //This map is for latency values of this datacenter with other datacetners. Each entry is destination datacenter id and egress letancy value.
	double cohostedLatency;
	long averageCreationDelay;
	VMOffers vmOffers;
        
        
	public BigDatacenter(String name, DatacenterCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy,
			double ingressBandwidth, double ingressLatency, int basicCpuUnit, long averageCreationDelay, VMOffers vmOffers, long seed) throws Exception {
		super(name,characteristics,vmAllocationPolicy,null,0);
                
		this.vmTable = new Hashtable<Integer,SVM>();
		this.basicCpuUnit = basicCpuUnit;
		this.vmCreationTime = new Hashtable<SVM,Long>();
		this.vmPrice = new Hashtable<SVM,Double>();
				
		this.random = new Random(seed);
		this.ingressBandwidth = ingressBandwidth;
		this.averageCreationDelay = averageCreationDelay;
		//if latency is smaller than minimum quantum of time, ignore it
		if (ingressLatency<QUANTUM) ingressLatency = 0.0;
		this.ingressLatency = ingressLatency;
		this.vmOffers = vmOffers;
		destDatacenterEgressBwMap=new HashMap<>();
                destDatacenterEgressLatMap=new HashMap<>();
		//latency for transmission within a host is 10% of the network latency
		this.cohostedLatency = this.ingressLatency/10;
		//if(this.cohostedLatency<QUANTUM) cohostedLatency = 0.0;
		this.vmChannelTable = new Hashtable<Long,Channel>(); 
                this.destDatacenterChannelTable=new Hashtable<Long,Channel>();
	}
        
	@Override
	protected void processOtherEvent(SimEvent ev) {		
		if (ev == null){
			Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Null event ignored.");
		} else {
			int tag = ev.getTag();
			switch(tag){
				case UPDATE_NETWORK: updateNetwork(); break;
                                case EXSOURCE_STREAM: processEXSourceStream(ev); break;
				case TRANSFER_STREAM: processTransferStream(ev); break;
				case STREAM_AVAILABLE: processStreamAvailable(ev); break;
				case CloudSimTags.END_OF_SIMULATION: shutdownEntity(); break;
				default: Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Unknown event ignored. Tag: "+tag);
			}
		}
	}
	
	@Override
	protected void processVmCreate(SimEvent ev, boolean ack) {
		super.processVmCreate(ev, false);//do not send ack to broker yet
		
		SVM svm = (SVM) ev.getData();
		Log.printLine(CloudSim.clock()+": SVM #"+svm.getId()+" created.");
		vmCreationTime.put(svm, (long) CloudSim.clock());
		vmPrice.put(svm, getPrice(svm));
		vmTable.put(svm.getId(), svm);
		
		/*send the ack to GraphAppEngine with a random delay, to
		 * model delay in VM boot time (The charge should be started from the
		 * moment we request the Vm, but it takes a while for it
		 * to be usable)
		 */
		double randomDelay = Math.ceil(averageCreationDelay*(1.0+random.nextGaussian()/10.0));
		int[] data = new int[3];
		data[0] = getId();
		data[1] = svm.getId();
		data[2] = CloudSimTags.TRUE;

		send(svm.getUserId(),randomDelay,CloudSimTags.VM_CREATE_ACK, data);
	}
	
	@Override
	protected void processVmDestroy(SimEvent ev, boolean ack) {
		//do last accounting for the vm
		SVM vm = (SVM) ev.getData();
		long startTime = vmCreationTime.remove(vm);
		double price = vmPrice.remove(vm);
		//long useInHours = updateVmUsage(startTime,price);
                
		super.processVmDestroy(ev, ack);
	}
						
	@Override	
	protected void processCloudletSubmit(SimEvent ev, boolean ack) {
		updateCloudletProcessing();
		try {
			// gets the Cloudlet object
			ServiceCloudlet cl = (ServiceCloudlet) ev.getData();
                        
			// process this Cloudlet to this CloudResource
			cl.setResourceParameter(getId(), 0.0, 0.0);

			int userId = cl.getUserId();
			int vmId = cl.getVmId();
			Host host = getVmAllocationPolicy().getHost(vmId, userId);
                        
			SVM svm = (SVM) host.getVm(vmId, userId);
			ServiceCloudletSchedulerSpaceShared scheduler = (ServiceCloudletSchedulerSpaceShared) svm.getCloudletScheduler();
			double estimatedFinishTime = scheduler.cloudletSubmit(cl);
			if (estimatedFinishTime<QUANTUM) estimatedFinishTime=QUANTUM;
                        
			send(getId(),estimatedFinishTime,CloudSimTags.VM_DATACENTER_EVENT);
			if (ack) {
				int[] data = new int[3];
				data[0] = getId();
				data[1] = cl.getCloudletId();
				data[2] = CloudSimTags.TRUE;

				// unique tag = operation tag
				sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data);
			}
		} catch (Exception e) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
			e.printStackTrace();
		}

		//checkCloudletCompletion();
	}
		
	@Override
	protected void updateCloudletProcessing() {
            
            if (CloudSim.clock() >= this.getLastProcessTime()+QUANTUM) {
                    double smallerTime = Double.MAX_VALUE;
                    for (Host host: getVmAllocationPolicy().getHostList()){
                            double time = host.updateVmsProcessing(CloudSim.clock());
                            if (time < smallerTime) smallerTime = time;
                    }

                    //if there are more events
                    if (smallerTime < Double.MAX_VALUE) {
                            double delay = smallerTime-CloudSim.clock();
                            if (delay<QUANTUM) delay=QUANTUM;
                            //delay=QUANTUM;
                            //JOptionPane.showMessageDialog(null, smallerTime + "\n" + CloudSim.clock());
                            schedule(getId(), delay, CloudSimTags.VM_DATACENTER_EVENT);
                    } 

                    setLastProcessTime(CloudSim.clock());
            }

            for(Host host: getVmAllocationPolicy().getHostList())
            {
                for(Vm vm: host.getVmList())
                {
                    SVM svm=(SVM) vm;
                    if(!svm.outputQueue.isEmpty()) 
                    {//if outputQueue is not empty, meaning that a new output stream(s) is/are generated and need to be sent to corrsponding services cloudlets (VMs)

                        Stream s=null;
                        while(!svm.outputQueue.isEmpty())
                        {
                            //dequeue stream from outputQueue
                            try{
                                s=svm.getStreamFromOutputQueue(); 
                            }catch(Exception ex){
                            }

                            if(s!=null)
                            {

                                HashMap<Stream,Integer> streamPortionVMIdMap= StreamSchedulingOnSVMs.getStreamPortionsSchedulingOnVMs(s);

                                for(Stream streamPortion: streamPortionVMIdMap.keySet())
                                {
                                    int desVMID=streamPortionVMIdMap.get(streamPortion);
                                    int destDatacenterid = StreamSchedulingOnSVMs.getDatacenterID(desVMID);
                                    //Stream streamPortion=vmStreamPortionMap.get(desVMID); //stream portion
                                    TransferStreamEvent event = new TransferStreamEvent(streamPortion , getId(), svm.getId(), destDatacenterid , desVMID);
                                    //JOptionPane.showMessageDialog(null, s.id +  "" + destDatacenterID + " " + destinationVMID);
                                    //JOptionPane.showMessageDialog(null, streamPortion.getId() +  "\n" + streamPortion.getPortionID() + "\n" + CloudSim.clock());
                                    sendNow(getId(),TRANSFER_STREAM,event);
                                }
                            }
                            else
                                Log.printLine("Dequeue stream from OutputQueue of SVM #"  + svm.getId() + " is failied");
                        }
                    }
                }
            }

	}
	
	private void updateNetwork() {
		double smallerEvent = Double.POSITIVE_INFINITY;
                
                //Update engress channel
                for(Channel channel: destDatacenterChannelTable.values()){
                        //JOptionPane.showMessageDialog(null, "Egress");
			//updates each channel
			if (CloudSim.clock() > channel.getLastUpdateTime()+QUANTUM) {
				double nextEvent = channel.updateTransmission(CloudSim.clock());
				if (nextEvent<smallerEvent) {
					smallerEvent = nextEvent;
				}
			}
			
			//process arrived streams
			LinkedList<StreamTransmission> arrivingList= channel.getArrivedStreams();
			for (StreamTransmission tr: arrivingList){
                            //Sent stream and make it avalaible at destination datacenter, considering datacenter latency
                            //transmissionDealy is not needed to add it becuase it is already considered when updateNetwork event is scheduled by addTransmission method
                            double engressLatencyForDestDatacenter= destDatacenterEgressLatMap.get(tr.getDestinationDatacenterId());
                            schedule(tr.destinationDatacenterId,engressLatencyForDestDatacenter,STREAM_AVAILABLE,tr);
			}
		}
                
                //Update ingress channel
                for(Channel channel: vmChannelTable.values()){
			//updates each channel
			if (CloudSim.clock() > channel.getLastUpdateTime()+QUANTUM) {
				double nextEvent = channel.updateTransmission(CloudSim.clock());
				if (nextEvent<smallerEvent) {
					smallerEvent = nextEvent;
				}
			}
			
			//process arrived dataItems
			LinkedList<StreamTransmission> arrivingList= channel.getArrivedStreams();
			for (StreamTransmission tr: arrivingList){ //Note: tr.destinationDatacenterId = getId(), either one can be used
                            //Sent stream and make it avalaible at destination datacenter, considering datacenter latency
                            schedule(tr.destinationDatacenterId,ingressLatency,STREAM_AVAILABLE,tr); //or schedule(tr.destinationDatacenterId,latency,DATA_ITEM_AVAILABLE,tr);
			}
		}
		
		if(smallerEvent!=Double.POSITIVE_INFINITY){
			if (smallerEvent-CloudSim.clock()<QUANTUM) smallerEvent = CloudSim.clock()+QUANTUM;
			schedule(getId(),smallerEvent,UPDATE_NETWORK);
		}
	}
	
        private void processEXSourceStream(SimEvent ev)
        {
            Object[] data=(Object[]) ev.getData();
            int vmid=(int) data[0];
            Stream stream = (Stream) data[1];
            
            StreamTransmission transmission = new StreamTransmission(stream,getId(), vmid, getId(),vmid);  
            schedule(getId(),ingressLatency,STREAM_AVAILABLE,transmission);
        }
        
	private void processTransferStream(SimEvent ev) {
            
                //Create channel either is ingress or engress if it does not exist
		TransferStreamEvent event = (TransferStreamEvent) ev.getData();
                int sourceDatacenterId=event.getSourceDatacenterId(); //of course, source datacenter is this datacenter (i.e. the sender of event) so you can use getId() instead
		int sourceVMId = event.getSourceVMId();
                int destinationDatacenterId=event.getDestinationDatacenterId();
		int destinationVMId = event.getDestinationVMId();
		Stream stream = event.getStream();
		
		//check if VMs are cohosted
		boolean cohosted=true;
                if(sourceDatacenterId!=destinationDatacenterId) // transfer stream between different datacenters
                {
                    createChannel(sourceDatacenterId, sourceVMId, destinationDatacenterId);
                    cohosted=false;
                }
                else //transfer stream between VMs located in the same datacenter
                {
                    if (vmTable.get(sourceVMId).getHost().getId()!=vmTable.get(destinationVMId).getHost().getId()) //VMs are not in the same host
                    { 
                        //Note: sourceDatacenterId = destinationDatacenterId as the transmission is within the same datacenter
                        //sourceVm and destination VM located at different host
                        createChannel(sourceDatacenterId, sourceVMId, destinationDatacenterId);
                        cohosted=false;
                    }
                    else
                    {
                        //nothing, there is no need for creating channel and the value of cohosted flag is already true
                    }
                }
                
                
                //Update network
                updateNetwork();
                                
                //Add stream transmission
                addStreamTransmission(stream, sourceDatacenterId, sourceVMId, destinationDatacenterId, destinationVMId, cohosted);
	}

	private void processStreamAvailable(SimEvent ev) {
                StreamTransmission tr=(StreamTransmission) ev.getData();
                
                int owner = tr.getstream().getOwnerId();

                List<SVM> svms=getVmList();
                SVM svm=null;
                for(int i=0;i<svms.size();i++)
                    if(svms.get(i).getId()==tr.getDestinationVMId())
                        svm=svms.get(i);

                 //= (SVM) getVmList().get(tr.getDestinationVMId());
                if(svm!=null)
                {
                    svm.addStreamToInputQueue(tr.getstream());

                    //Using canonicalID
                    if(tr.getstream().isPortion())
                        Log.printLine(CloudSim.clock()+": Stream #"+tr.getstream().getId()+" Portion#" + tr.getstream().portionID + " is now available at VM #"+tr.getDestinationVMId() + " in Datacenter #" + GraphAppEngine.getCanonicalIDForDataCenter(tr.getDestinationDatacenterId()) + " --- Sent by VM #"  +tr.getSourceVMId() + " in Datacenter #" + GraphAppEngine.getCanonicalIDForDataCenter(tr.getSourceDatacenterId()));
                    else
                        Log.printLine(CloudSim.clock()+": Stream #"+tr.getstream().getId()+" is now available at VM #"+tr.getDestinationVMId() + " in Datacenter #" + GraphAppEngine.getCanonicalIDForDataCenter(tr.getDestinationDatacenterId()) + " --- Sent by VM #"  +tr.getSourceVMId() + " in Datacenter #" + GraphAppEngine.getCanonicalIDForDataCenter(tr.getSourceDatacenterId()));

                    //To put stream coming in corssponding VM (i.e. calling processingCloudlet method)
                    sendNow(getId(),CloudSimTags.VM_DATACENTER_EVENT);
                }
	}

	private void addStreamTransmission(Stream data, int sourceDatacenterId, int sourceVMid, int destinationDatacenterId, int destinationVMId, boolean cohosted) {
		
                StreamTransmission transmission = new StreamTransmission(data, sourceDatacenterId, sourceVMid, destinationDatacenterId, destinationVMId);
		if(cohosted){
			//vms are in the same host; Just add a small latency
			schedule(sourceDatacenterId,cohostedLatency,STREAM_AVAILABLE,transmission);
		} else {
                        //vms are not cohosed, which means that they may be in the same datacenter at different host, or may be at different datacenterse 

			double transmissionDelay = getChannel(sourceDatacenterId, sourceVMid, destinationDatacenterId, destinationVMId).addTransmission(transmission);
                        //JOptionPane.showMessageDialog(null, transmissionDelay);
                        if(sourceDatacenterId == destinationDatacenterId) //transmission within the same datacenter
                        {
                            if (transmissionDelay>=QUANTUM){
                                    //schedules a completion event
                                    schedule(sourceDatacenterId,transmissionDelay,UPDATE_NETWORK);
                            } else 
                                {
                                    //very short transmission since transmissionDealy is less than QUANTUM, so ignore it and use the larency; remove from transmission
                                    getChannel(sourceDatacenterId, sourceVMid, destinationDatacenterId, destinationVMId).removeTransmission(transmission);
                                    schedule(sourceDatacenterId,ingressLatency,STREAM_AVAILABLE,transmission);
                                 }
                        }
                        else {
                                if (transmissionDelay>=QUANTUM){
                                    schedule(sourceDatacenterId,transmissionDelay,UPDATE_NETWORK);
                                }
                                else
                                {
                                    //negligible transmission delay, just consider link latency
                                    getChannel(sourceDatacenterId, sourceVMid, destinationDatacenterId, destinationVMId).removeTransmission(transmission);
                                    double engressLatecyForDestDatacenter= destDatacenterEgressLatMap.get(destinationDatacenterId);
                                    schedule(transmission.getDestinationDatacenterId(),engressLatecyForDestDatacenter,STREAM_AVAILABLE,transmission);
                                }
			}
		}
	}
        
        private void createChannel(int sourceDatacenterId, int sourceVMId, int destinationDatacenterId){
                //Note: source datacenter id is this datacenter, so sourceDatacenterId=getId() - Thus, you can use either sourceDatacenterId or getId()
                long key=0;
                if(sourceDatacenterId == destinationDatacenterId) //Ingress channel
                {
                    key = sourceVMId;
                    
                    if(!vmChannelTable.containsKey(key)){
                        Channel channel = new Channel(ingressBandwidth);
                        vmChannelTable.put(key, channel);
                    }
                    
                }
                else //Egress channel
                {
                    key = destinationDatacenterId;
                    
                    if(!destDatacenterChannelTable.containsKey(key)){ //Egress channel
                        Channel channel = new Channel(destDatacenterEgressBwMap.get(destinationDatacenterId));
			destDatacenterChannelTable.put(key, channel);
                    }
                }
	}
        
	
        private Channel getChannel(int sourceDatacenterId, int sourceVMId, int destinationDatacenterId, int destinationVMId){
		
                Channel channel=null;
                long key=0;
                if(sourceDatacenterId == destinationDatacenterId)
                {
                    key = sourceVMId;
                    channel=vmChannelTable.get(key);
                }
                else
                {
                    key = destinationDatacenterId;
                    channel=destDatacenterChannelTable.get(key);
                }
                return channel;
	}
	
	private double getPrice(SVM vm) {
		LinkedHashMap<Vm,Double> vmOffersTable = vmOffers.getVmOffers();
		double cost=0.0;
		
                cost=vmOffers.getCost(vm.getMips(), vm.getNumberOfPes(),vm.getRam(), vm.getBw());
                
		return cost;
	}
		
	//Simulation output
	public void printSummary(){
		DecimalFormat df = new DecimalFormat("#.##");
		Log.printLine();
		Log.printLine("======== DATACENTER #"+  GraphAppEngine.getCanonicalIDForDataCenter(getId()) +" SUMMARY ========");
                //Log.printLine("======== DATACENTER #"+  StreamSchedulingOnVMs.getIncrementialDataCenterID(getId()) +" SUMMARY ========");
		//Log.printLine("= Cost: $"+df.format(cost));
		Log.printLine("========== END OF SUMMARY =========");
	}

        public HashMap<Integer, Double> getDestDatacenterEgressBwMap() {
            return destDatacenterEgressBwMap;
        }

        public HashMap<Integer, Double> getDestDatacenterEgressLatMap() {
            return destDatacenterEgressLatMap;
        }
        
        
}
