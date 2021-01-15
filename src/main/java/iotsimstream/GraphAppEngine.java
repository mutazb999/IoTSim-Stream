package iotsimstream;

import iotsimstream.vmOffers.VMOffers;
import iotsimstream.schedulingPolicies.Policy;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

/**
 * This class handles the whole process of stream graph application (aka stream workflow)
 * execution, including: parsing xml file, defining
 * number, types, and start times of vms, and scheduling,
 * dispatching, and management of DAG tasks.
 * 
 * @author Mutaz Barika
 */
public class GraphAppEngine extends SimEntity{

        private static final int START_DELAY = 998800;
        private static final int DO_PROVISIONING_AND_SCHEDULING=998802;
        protected final static double minDPUnit = 1; //minimum stream processing unit (in MB per second, e.g. processing at least 0.1 MB/s)
        private double requestedSimulationTime; //Unit: in seconds
        private static double startupTime; //Unit: in seconds
	private double actualStartTime = 0.0;
	private double endTime;
	private String dagFile;
	private Policy policy;
	
        private GraphAppClouldlet graphAppCloudlet;
	private HashMap<Integer,Boolean> freeVmList;
	private HashMap<Integer,Boolean> runningVmList; 
	private HashMap<ServiceCloudlet,Service> cloudletServicekMap;
	private Hashtable<Integer,HashSet<Integer>> streamRequiredLocation;
	private Hashtable<Integer,ArrayList<Integer>> schedulingTable;
	private Hashtable<Integer,SVM> vmTable;
        private List<? extends ServiceCloudlet> cloudletReceivedList;
        private ArrayList<ExternalSource> externSources;
        private HashMap<Integer, Service> serviceMap;
        private HashMap<Integer, Integer> ProSVMDatacenterMap;
        private ArrayList<Integer> datacenters;
        private HashMap<Integer, Boolean> datacenterInitializeMap;
        private LinkedHashMap<Integer, VMOffers> datacenterWithVMOffers; //needed to preserver the order for later use
        private ArrayList<Service> services;
        int datacenterCount; //incremental count for datacenters starting from 0
        static double totalProcessedStreams;
        
        /**
         * The map between CloudSim assigned IDS for dataceners and incremental counters for these datacenters.
         * Each key is a CloudSim datacenter ID and each value the corresponding
         * Datacenter counter.
         */
        protected static HashMap<Integer, Integer> datacenterIDsMap;

	public GraphAppEngine(String dagFile, Policy policy, double requestedST) {
		super("GraphAppEngine");
		setCloudletReceivedList(new ArrayList<ServiceCloudlet>());
		this.dagFile = dagFile;
		this.policy = policy;
		this.requestedSimulationTime=requestedST;
                startupTime=0;
                this.datacenterCount=0;
                
		this.freeVmList = new HashMap<Integer,Boolean>();
		this.runningVmList = new HashMap<Integer,Boolean>();
		this.cloudletServicekMap = new HashMap<ServiceCloudlet,Service>();
		this.vmTable = new Hashtable<Integer,SVM>();
                
                
                streamRequiredLocation=new Hashtable<Integer,HashSet<Integer>>();
                externSources=new ArrayList<ExternalSource>();
                serviceMap=new HashMap<Integer, Service>();
                datacenters=new ArrayList<Integer>();
                datacenterInitializeMap=new HashMap<>();
                datacenterWithVMOffers=new LinkedHashMap<Integer,VMOffers>();
                ProSVMDatacenterMap=new HashMap<Integer,Integer>();
                datacenterIDsMap=new HashMap<Integer, Integer>();
                services=new ArrayList<Service>();
                totalProcessedStreams=0.0;
                
                //Initialize StreamSchedulingOnSVMs object
                StreamSchedulingOnSVMs.init();
	}

	@Override
	public void processEvent(SimEvent ev) {
		if (ev == null){
			Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Null event ignored.");
		} else {
			int tag = ev.getTag();
			switch(tag){
				case CloudSimTags.RESOURCE_CHARACTERISTICS: processResourceCharacteristics(ev); break;
                                case DO_PROVISIONING_AND_SCHEDULING: doProvisioningAndScheduling(); break;
                                case CloudSimTags.VM_CREATE_ACK: processVmCreate(ev); break;
				case START_DELAY:	processStartDelay(); break;
                                case CloudSimTags.END_OF_SIMULATION: processEndOfSimulation(); break;
				default: Log.printLine("Warning: "+CloudSim.clock()+": "+this.getName()+": Unknown event ignored. Tag: "+tag);
			}
		}
	}

        
        protected void processResourceCharacteristics(SimEvent ev)
        {
            //Get datacenter characteristics and add its id in the list (i.e. add datacenter ID in the list)
            DatacenterCharacteristics characteristics=(DatacenterCharacteristics) ev.getData();
            int datacenterid=characteristics.getId();
            datacenters.add(datacenterid);  
            datacenterIDsMap.put(datacenterid, datacenterCount);
            datacenterInitializeMap.put(datacenterid, Boolean.TRUE);
            datacenterCount++;
            
            //Check if all datcenters are intialized and their characteristics are received
            boolean check=true;
            for(Integer id: datacenterInitializeMap.keySet())
            {
                if(datacenterInitializeMap.get(id)== false)
                {
                    check=false;
                    break;
                }
            }
            if(check) //all datacenters are intialized so that we need to intialize bandwidth and dealy matrices and then do provisioning
            {
                //Send DO_PROVISIONING_AND_SCHEDULING event
                sendNow(getId(), DO_PROVISIONING_AND_SCHEDULING);
            }
        }
        
        public static int getCanonicalIDForDataCenter(int canonicalDatacenterID)
        {
            if(canonicalDatacenterID>=0 && datacenterIDsMap.size()>0)
                return datacenterIDsMap.get(canonicalDatacenterID);
            return -1;
        }
        
	// all the simulation entities are ready: start operation
	protected void doProvisioningAndScheduling(){

                //set VMOffers
                collectVMOffers();
            
		Log.printLine(CloudSim.clock()+": Graph application execution started.");
		actualStartTime = CloudSim.clock();

		policy.processDagFileAndScheduling(dagFile,this.getId(), datacenterWithVMOffers);
		streamRequiredLocation = policy.getDataRequiredLocation();
		schedulingTable = policy.getScheduling();
		ArrayList<ProvisionedSVm> vms = policy.getProvisioning();
                services = policy.getServices();
                
                //Initilize GraphAppCloudlet for this graph application
                graphAppCloudlet=new GraphAppClouldlet(services);
                
                //Get external sources
                externSources=policy.getExternalSources();
                
		//trigger creation of vms
		createVMs(vms);
                
                //fill ServiceMap, each entry is service id and service object
                for(Service service: services)
                    serviceMap.put(service.getId(), service);
                    
                //Initilize StreamSchedulingOnSVMs class -----------------------------
                //Set datacenter canonical IDs
                StreamSchedulingOnSVMs.setDatacenterCanonicalIDsMap(datacenterIDsMap);                
                //Set StreamRequiredLocation map in VMDatacenterAndStreamLocations
                StreamSchedulingOnSVMs.setStreamRequiredLocation(streamRequiredLocation);
                //Set ProSVMDatacenterMap in VMDatacenterAndStreamLocations
                StreamSchedulingOnSVMs.setProSVMDatacenterIDsMap(ProSVMDatacenterMap);
                //Set services in 
                StreamSchedulingOnSVMs.setServices(services);
                //Set vmTable for mapping of vmid and SVM
                StreamSchedulingOnSVMs.setVMTable(vmTable);
                //Compute RR values for cloudlets of all services
                StreamSchedulingOnSVMs.computeRRValuesForServicesCloudlets();
	}

        private void createVMs(ArrayList<ProvisionedSVm> vms)
        {
            for (ProvisionedSVm pvm: vms){
                SVM svm = pvm.getVm();

                freeVmList.put(svm.getId(), false);
                runningVmList.put(svm.getId(), false);
                vmTable.put(svm.getId(), svm);

                //fill ProSVMDatacenterMap, where each entry is the VMID with corrsponding DatacenterID (VM runing corrsponding datacenter)
                ProSVMDatacenterMap.put(svm.getId(), pvm.datacenterID);

                send(pvm.datacenterID,actualStartTime+pvm.getStartTime(),CloudSimTags.VM_CREATE_ACK,svm);
            }
        }
        
        private void collectVMOffers()
        {
            for(Integer datacenterID: datacenters)
            {
                BigDatacenter datacenter=(BigDatacenter) CloudSim.getEntity(datacenterID);
                datacenterWithVMOffers.put(datacenterID, datacenter.vmOffers);
                datacenter.vmOffers.getVmOffers(); //this statement is required to fill vmOffersTable (initilization) for vmOffer object
            }
        }
        
	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int vmId = data[1];
		
		freeVmList.put(vmId,true);
		runningVmList.put(vmId, true);
		
                dispatchServiceCloudlets();
                
                //Check if all VMs are created before starting send streams from external sources
                boolean check=true;
                for(Integer id: runningVmList.keySet())
                {
                    if(runningVmList.get(id)==false) // means this VM not created
                        check=false;
                }
                
                if(check) //all VMs are created and cloudlets are dispatched, so we need (1) to set startupt time (time for creating VMs and dispached cloudlets) in order to be used when calculated stop time (requestedSimulationTime+startupTime) and (2) to triger external sources to corrsponding services to start sending streams 
                {
                    startupTime=CloudSim.clock();
                    Log.printLine("-------------------------------------------");
                    Log.printLine("= Startup time (parsing, scheduling plan, VM creation and dispatching) :" + startupTime);
                    System.out.println("-------------------------------------------");
                    System.out.println("= Startup time (parsing, scheduling plan, VM creation and dispatching) :" + startupTime);
                    Log.printLine("= Startup time plus requested simulation time:" + (requestedSimulationTime+startupTime));
                    Log.printLine("-------------------------------------------");
                    System.out.println("= Startup time plus requested simulation time:" + (requestedSimulationTime+startupTime));
                    System.out.println("-------------------------------------------");
                    Log.printLine(" ========== Simulation begins ==========");
                    System.out.println(" ========== Simulation begins ==========");
                    
                    //Schedule stop simulation time (end of siulation event) according to requested time
                    schedule(getId(), requestedSimulationTime, CloudSimTags.END_OF_SIMULATION);
                    
                    //Start sending streams from external sources
                    startEXSourceStreams();
                }
	}
	
	private void processStartDelay() {
	
                //Add all datacenters ids to datacenterInitializeMap and their values are false in order to allow checking their initialization later on (when receiving reosurce_characteristics event from each one)
                for(Integer resid: CloudSim.getCloudResourceList())
                {
                  BigDatacenter datacenter=(BigDatacenter) CloudSim.getEntity(resid);
                  datacenterInitializeMap.put(datacenter.getId(), Boolean.FALSE);  
                }
                
                // we gave data center enough time to initialize. Start the action...
                for(Integer resid: CloudSim.getCloudResourceList())
                {
                  BigDatacenter datacenter=(BigDatacenter) CloudSim.getEntity(resid);
                  sendNow(datacenter.getId(), CloudSimTags.RESOURCE_CHARACTERISTICS, getId());
                }
	}

	@Override
	public void shutdownEntity() {
		//do nothing
	}

	@Override
	public void startEntity() {
            
            //give time to the data center to start (two seconds delay is used)
            send(getId(),2,START_DELAY);
	}
	
        private void dispatchServiceCloudlets() {

            for(int serviceId:schedulingTable.keySet()){
                for(Integer vmid:schedulingTable.get(serviceId)){
                    if (freeVmList.get(vmid)==true){//this vm is available
                        Service service = serviceMap.get(serviceId);

                        if (service!=null) {
                                ServiceCloudlet cl= service.getServiceCloudletByVM(vmid);
                                System.out.println(CloudSim.clock()+": ServiceCloudlet#" + cl.getCloudletId() + " - Service#"+service.getId() + ": dispatched to VM#"+ vmid);
                                Log.printLine(CloudSim.clock()+": ServiceCloudlet#" + cl.getCloudletId() + " - Service#"+service.getId() + ": dispatched to VM#"+ vmid);
                                schedulingTable.get(serviceId).remove(vmid);//remove vmid from the scheduling table since instance of such service being scheduled on this vm
                                freeVmList.put(vmid, false); //vm is busy now
                                cloudletServicekMap.put(cl, service);
                                getCloudletReceivedList().add(cl);
                                int datacenterID=StreamSchedulingOnSVMs.getDatacenterID(vmid); 
                                sendNow(datacenterID,CloudSimTags.CLOUDLET_SUBMIT,cl);
                                break;
                        }              
                    }
                }
            }
        }
	
        protected <T extends ServiceCloudlet> void setCloudletReceivedList(List<T> cloudletReceivedList) {
            this.cloudletReceivedList = cloudletReceivedList;
        }
		
	@SuppressWarnings("unchecked")
	public <T extends ServiceCloudlet> List<T> getCloudletReceivedList() {
		return (List<T>) cloudletReceivedList;
	}
		 
	//Output information supplied at the end of the simulation
	public void printExecutionSummary() {
		DecimalFormat dft = new DecimalFormat("#####.##");
		String indent = "\t";
		
		Log.printLine("========== WORKFLOW EXECUTION SUMMARY ==========");
		Log.printLine("= Cloudlet " + indent + "Status" + indent + indent + "Submission Time" + indent + "Execution Time (s)" + indent + "Finish Time");
		for (ServiceCloudlet cloudlet: getCloudletReceivedList()) {
			Log.print(" = "+cloudlet.getCloudletId() + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");
				double executionTime = cloudlet.getFinishTime()-cloudlet.getExecStartTime();
				Log.printLine(indent + indent + dft.format(cloudlet.getSubmissionTime()) + indent + indent + dft.format(executionTime) + indent + indent + dft.format(cloudlet.getFinishTime()));
			} else if (cloudlet.getCloudletStatus() == Cloudlet.FAILED) {
				Log.printLine("FAILED");
			} else if (cloudlet.getCloudletStatus() == Cloudlet.CANCELED) {
				Log.printLine("CANCELLED");
			}
		}
		
                Log.printLine("= Startup Time: "+ startupTime); //Unit: seconds
                Log.printLine("= Requested Experiment Time: "+ requestedSimulationTime); //Unit: seconds
		Log.printLine("= Finish time: "+ endTime); //Unit: seconds
                DecimalFormat newdft2 = new DecimalFormat("############.##"); 
		Log.printLine("= Total processed streams: "+ newdft2.format(totalProcessedStreams) +" MB"); //Unit: MB
		Log.printLine("========== END OF SUMMARY =========");
		Log.printLine();
	}
	
        protected void startEXSourceStreams()
        {
            for(int i=0;i<externSources.size();i++)
            {
                ExternalSource ex=externSources.get(i);
                int exid=ex.getId();
                sendNow(exid,ExternalSource.SEND_STREAM);
            }
        }
        
        protected void processEndOfSimulation()
        {   
            double totalOfProcessedStreams=0.0;
            
            String printTotalOfProcessedStreamsPerCloudlet="";
            //Stop sending more streams from External Sources
            for(ExternalSource ex: externSources)
            {
                sendNow(ex.getId(), ExternalSource.STOP_SENDING_STREAM);
            }
            
            //Stop services and their cloudlets
            for(Integer serviceid: serviceMap.keySet())
            {
                Service service=serviceMap.get(serviceid);
                try {
                    //Make status of Cloudlet as SUCCESS to stop running service (stope service) 
                    for(ServiceCloudlet cl: service.getServiceCloudlets())
                    {
                        cl.setCloudletStatus(Cloudlet.SUCCESS);
                        totalOfProcessedStreams+=cl.totalOfProcessedStream;
                        printTotalOfProcessedStreamsPerCloudlet = printTotalOfProcessedStreamsPerCloudlet + "ServiceCloudlet#" + cl.getCloudletId() + " is processed in totoal " + cl.totalOfProcessedStream + "\n";
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            
            //Stop VMs
            for(int id:runningVmList.keySet()){
                SVM svm = vmTable.remove(id);
                //get datacenterID for this SVM
                int datacenterID= ProSVMDatacenterMap.get(svm.getId()); //or StreamSchedulingOnVMs.getDatacenterID(svm.getId())
                sendNow(datacenterID,CloudSimTags.VM_DESTROY,svm);
                schedulingTable.remove(id);
                freeVmList.remove(svm);
                runningVmList.put(svm.getId(),false);
            }
            
            Log.printLine(CloudSim.clock()+": Workflow execution finished.");
            endTime =  CloudSim.clock();
            
            totalProcessedStreams=totalOfProcessedStreams;
        }
        
        public static double getMinDPUnit()
        {
            return minDPUnit;
        }
}