package iotsimstream.schedulingPolicies;

import iotsimstream.ExternalSource;
import iotsimstream.GraphAppEngine;
import iotsimstream.ProvisionedSVm;
import iotsimstream.Service;
import iotsimstream.Stream;
import iotsimstream.vmOffers.VMOffers;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.cloudbus.cloudsim.Log;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

/**
 * This class implements the abstract policy for provisioning and scheduling of a DAG
 * in an IaaS data center. This method performs common tasks such as parsing the XML
 * file describing the DAG, printing the schedule, and returning provisioning and
 * scheduling decisions to the Graph Application Engine. The abstract method must implement the
 * logic for filling the data structures related to provisioning and scheduling decisions. 
 *
 */
public abstract class Policy extends DefaultHandler {
	
	protected int ownerId;
	int serviceCount;
        final double minDPUnit=GraphAppEngine.getMinDPUnit(); 
	
	/*Data structures filled during XML parsing*/
	ArrayList<Stream> originalDataItems;
	ArrayList<Service> entryServices;
	ArrayList<Service> exitServices;
	ArrayList<Service> services;
        ArrayList<ExternalSource> externalSources;
        private LinkedHashMap<Integer, VMOffers> datacenterWithVMOffers;
	Hashtable<Integer,HashSet<Integer>> streamRequiredLocation;
	Hashtable<Integer,ArrayList<Integer>> schedulingTable; //each entry is service (serviceid) and list of vms (vmids), where coudlets of such service will be scheduled on these vms, one cloudlet per vm
	ArrayList<ProvisionedSVm> provisioningInfo;
        String id="";
        
        /**
	 * Fills the provisioning and scheduling data structures that are supplied
	 * to the Graph Application Engine.
	 * @param datacentersWithVMOffers the VMOffers object that encapsulates information on available IaaS instances
	 */
        public abstract void doScheduling(LinkedHashMap<Integer, VMOffers> datacentersWithVMOffers);

	public Policy(){

	}
	
	/**
	 * Reads the file specified as input, and processes the corresponding DAG, generating
	 * internal representation of provisioning and scheduling decision. WorkflowEngine
	 * queries for such an information to process the DAG.
	 *   
	 * @param dagFile Name of the DAG file.
         * @param ownerId ID of onwer.
         * @param datacenterWithVMOffers List of VM offers from IaaS clouds.
	 */
	public void processDagFileAndScheduling(String dagFile, int ownerId, LinkedHashMap<Integer, VMOffers> datacenterWithVMOffers){
                this.ownerId = ownerId;
                
		this.datacenterWithVMOffers=datacenterWithVMOffers;
		this.originalDataItems = new ArrayList<Stream>();
		this.entryServices = new ArrayList<Service>();
		this.exitServices = new ArrayList<Service>();
		this.services = new ArrayList<Service>();
		
                externalSources=new ArrayList<ExternalSource>();
                
		this.streamRequiredLocation = new Hashtable<Integer,HashSet<Integer>>();
		this.schedulingTable = new Hashtable<Integer,ArrayList<Integer>>();
		this.provisioningInfo = new ArrayList<ProvisionedSVm>();
		
		SAXParserFactory spf = SAXParserFactory.newInstance();
		try {
			SAXParser sp = spf.newSAXParser();
			sp.parse(dagFile, this);		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	/**
	 * Determines the VMs where each dataItem will be required.
	 * 
	 * @return A hashtable containing each registered dataItem and the list of VMs ids where they are required. 
	 */
	public Hashtable<Integer,HashSet<Integer>> getDataRequiredLocation(){
		return streamRequiredLocation;
	}
	
	/**
	 * Determines the ordering of Services execution inside each VM.
	 * 
	 * @return A hashtable containing each service (serviceid) and the list of VMs, in which cloudlets of such service will be executed on those VMs, one cloudlet per VM. 
	 */
	public Hashtable<Integer,ArrayList<Integer>> getScheduling(){
		return schedulingTable;
	}
		
	/**
	 * @return the list of required VMs (number, characteristics, start and end times)
	 */
	public ArrayList<ProvisionedSVm> getProvisioning(){
		return provisioningInfo;
	}

        /**
	 * @return the list of external sources
	 */
        public ArrayList<ExternalSource> getExternalSources() {
            return externalSources;
        }
	
        /**
	 * @return the list of services
	 */
        public ArrayList<Service> getServices()
        {
            return services;
        }
        
        
	public void printScheduling(double scheduleTime){
		Log.printLine("-------------------------------------------");
                System.out.println("-------------------------------------------");
		Log.printLine("-- Schedule time (ms):"+scheduleTime);
		System.out.println("-- Schedule time (seconds):"+scheduleTime);
                
		Log.printLine("-- Provisioning:");
                System.out.println("-- Provisioning:");
		for(ProvisionedSVm vm:provisioningInfo){
                        System.out.println("-- VM id:" + vm.getVm().getId() + " Datacenter id: " + GraphAppEngine.getCanonicalIDForDataCenter(vm.getDatacenterID()) + " RAM:"+vm.getVm().getRam()+
					" start:"+vm.getStartTime()+" end:"+vm.getEndTime());
                        Log.printLine("-- VM id:" + vm.getVm().getId() + " Datacenter id: " + GraphAppEngine.getCanonicalIDForDataCenter(vm.getDatacenterID()) + " RAM:"+vm.getVm().getRam()+
					" start:"+vm.getStartTime()+" end:"+vm.getEndTime());
		}
		
		Log.printLine("-- Scheduling:");
                System.out.println("-- Scheduling:");
                for (Service service:services){
                    Log.printLine("-- Service#"+ service.getId()+": ");
                    System.out.println("-- Service#"+ service.getId()+": ");
                    for(Integer vmid : schedulingTable.get(service.getId()))
                    {
                        Log.print("\tVM# " + vmid+": ");
                        System.out.print("\tVM# " + vmid+": ");
                        Log.print("ServiceCloudlet# " + service.getServiceCloudletByVM(vmid).getCloudletId()+" ");
                        System.out.print("ServiceCloudlet# " + service.getServiceCloudletByVM(vmid).getCloudletId()+" ");
                        Log.printLine();
                        System.out.println();
                    }
                }
		System.out.println();
		
		Log.printLine("-- Stream located at:");
                System.out.println("-- Stream located at:");
		for (Entry<Integer, HashSet<Integer>> entry: streamRequiredLocation.entrySet()){
			Log.print("-- Stream id#"+entry.getKey()+": ");
                        System.out.print("-- Stream id#"+entry.getKey()+": ");
			for(int loc:entry.getValue()){
                                Log.print("Service#" + loc+" ");
                                System.out.print("Service#" + loc+" ");
			}
			System.out.println();
                        Log.printLine();
		}
                Log.printLine();
		System.out.println();
		
		Log.printLine("-------------------------------------------");
                System.out.println("-------------------------------------------");
	}
			
	/********************************** SAX-related methods ****************************************/
	static Service currentService;
	static int cloudletCont;
	static int dataItemCont;
	static Hashtable<String,Service> xmlServiceIDsMap;
        static Hashtable<Integer,Service> serviceMap;
	static Hashtable<String,Stream> dataItems;
	ArrayList<Stream> generatedDataItems;
	
	public void startDocument(){
		currentService=null;
                serviceCount=0;
		cloudletCont=0;
		dataItemCont=0;
		xmlServiceIDsMap=new Hashtable<String,Service>();
                serviceMap= new Hashtable<Integer,Service>();
		dataItems=new Hashtable<String,Stream>();
		generatedDataItems=new ArrayList<Stream>();
	}
	
	public void startElement(String uri, String localName, String qName, Attributes attributes){
		/*
		 * Elements can be one of: 'adag' 'externalsources' 'exsource' 'service' 'uses' 'child' 'parent'
		 */
                
		if(qName.equalsIgnoreCase("adag")){//get number of services and set it to serviceCOunt variable in SimulationParameters class
                    String sCount = attributes.getValue("serviceCount");	
                } else if(qName.equalsIgnoreCase("externalsources")){//starting of external sources
                
		} else if(qName.equalsIgnoreCase("service")){//a new service is being declared
                    
                        id = attributes.getValue("id");
                        
                        //For data processing requirement
                        double serviceDPReq= Double.parseDouble(attributes.getValue("dataprocessingreq")); //milion instructions per MB (MI/MB)
                        
                        //For user data processing rate
                        double userDPRateReq = -1;
                        if(attributes.getValue("userreq") !=null)
                            userDPRateReq= Double.parseDouble(attributes.getValue("userreq")); //MB/s;
                        
                        Service service = new Service(serviceCount,ownerId,serviceDPReq,userDPRateReq);
			xmlServiceIDsMap.put(id, service);
                        serviceMap.put(service.getId(), service);
			services.add(service);
			entryServices.add(service);
			exitServices.add(service);
			
			currentService = service;
			cloudletCont++;
                        serviceCount++;
                        
                } else if(qName.equalsIgnoreCase("exsource")){//a external source dependency from the current service
                        String exid = attributes.getValue("id");
                        String exname = attributes.getValue("name");
                        
                        double datarate=Double.parseDouble(attributes.getValue("datarate")); //Unit MB/s
                        
                        //Create a new external source and set the id of stream according to the count 
                        ExternalSource externalsource= new ExternalSource(exname , dataItemCont, ownerId,datarate);
                        
                        externalSources.add(externalsource); //serviceid for external source is the cloudlet id
                        
                        Stream stream=externalsource.getStream();
                        originalDataItems.add(stream);
                        dataItems.put(exid, stream);
                        dataItemCont++;
                        
                } else if(qName.equalsIgnoreCase("uses")){//a stream dependency from the current service
			String link = attributes.getValue("link");
			
                        Stream stream = null;
                        
                        if(link.equalsIgnoreCase("input")) //input stream
                        {
                            //input could be from external source or service
                            String serviceRef = attributes.getValue("serviceref");
                            String producerRef = attributes.getValue("producerref");
                            
                            if(producerRef == null && serviceRef == null)
                            {
                                throw new IllegalArgumentException("Input for service is not defined. It should be either external source or service");
                            }
                            
                            else if(producerRef!=null) //input from external source
                            {
                                stream = dataItems.get(producerRef);
                            }
                            
                            else if(serviceRef !=null) //input from parent service
                            {
                                stream = dataItems.get(serviceRef);
                                
                                //Check processing type (replica or partition); Note that for our experiment we just using replica
                                String processingType = attributes.getValue("processingtype");
                                int sID=xmlServiceIDsMap.get(id).getId(); //this is serviceID used in simulation not the one in XML file
                                if(processingType.equalsIgnoreCase("replica"))
                                {
                                    //Replica processing tpye, so that the the current service id is add to replicaprocessing hashset
                                    stream.addReplicaProcessing(sID);
                                }
                                else //"partition" data mode
                                {
                                    String strPatitionPercentage = attributes.getValue("partitionprecentage");
                                    double patitionPercentage=Double.parseDouble(strPatitionPercentage);
                                    stream.addPartitionProcessing(sID, patitionPercentage);
                                }
                            }
                            else
                                throw new IllegalThreadStateException("Problem happen while parsing XML");
                        }
                        else //link = output, expecting service output stream
                        {
                            if (!dataItems.containsKey(id)){//stream not declared yet; register
                                //Get output stream datarate
                                double outputDataRate = Double.parseDouble(attributes.getValue("size"));//Long.parseLong(size);
                                
                                stream = new Stream(dataItemCont, ownerId, currentService.getId(),"service", outputDataRate);
                                originalDataItems.add(stream);
                                dataItems.put(id, stream);
                                dataItemCont++;
                                
                            } else { //stream already used by other service. Retrieve
                                    stream = dataItems.get(id);
                            }
                        }
                        
                        //Add stream to current service
                        if(link.equalsIgnoreCase("input")){
                            currentService.addStreamDependency(stream);
                        } else {
                                currentService.addOutput(stream);
                                generatedDataItems.add(stream);
                        }
		} else if(qName.equalsIgnoreCase("child")){//a service that depends on other(s)
			String ref = attributes.getValue("ref");
			currentService = xmlServiceIDsMap.get(ref);
			entryServices.remove(currentService);
		} else if(qName.equalsIgnoreCase("parent")){//a service that others depend on
			String ref = attributes.getValue("ref");
			Service parent = xmlServiceIDsMap.get(ref);
			
			parent.addChild(currentService);
			currentService.addParent(parent);
			exitServices.remove(parent);
		} else {
			Log.printLine("WARNING: Unknown XML element:"+qName);
		}
	}
		
	public void endDocument(){
            
                //parsing is completed. Cleanup auxiliary data structures and run the actual DAG provisioning/scheduling
		xmlServiceIDsMap.clear();
		dataItems.clear();
		originalDataItems.removeAll(generatedDataItems);
                
                
		long startTime = System.currentTimeMillis();
                doScheduling(datacenterWithVMOffers);
                
		double scheduleTime = System.currentTimeMillis()-startTime; //ms
                //scheduleTime = scheduleTime /1000; //converted to sec
		printScheduling(scheduleTime);
		
		//make sure original dataItems are available on the required vms
		for(Stream stream:originalDataItems){
			if (!streamRequiredLocation.containsKey(stream.getId())) 
                            streamRequiredLocation.put(stream.getId(), new HashSet<Integer>());
			HashSet<Integer> requiredAt = streamRequiredLocation.get(stream.getId());
				for(int at:requiredAt){
					//data.addLocation(at);
				}
		}

                
	}
}
