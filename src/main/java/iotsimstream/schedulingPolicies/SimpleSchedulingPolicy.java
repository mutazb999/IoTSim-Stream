package iotsimstream.schedulingPolicies;

import iotsimstream.ProvisionedSVm;
import iotsimstream.SVM;
import iotsimstream.Service;
import iotsimstream.ServiceCloudlet;
import iotsimstream.ServiceCloudletSchedulerSpaceShared;
import iotsimstream.Stream;
import iotsimstream.vmOffers.VMOffers;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import javax.swing.JOptionPane;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;

/**
 * This class implements a simple scheduling policy that select suitable VMs for each service that satisfies user performance requirements.
 *
 * @author Mutaz Barika
 */
public class SimpleSchedulingPolicy extends Policy {
			
	List<Vm> vmOffersList;
	
	int vmId=0;
        
        long provisioningSeed = 1310319;
        
        /** Note that: This class generates resource configuration solution, 
         * and then such solution being used to fill the following data structures:  provisioningInfo, schedulingTable, streamRequiredLocation, 
         * How it works: 
         * For each service, 
         *  create provisioned VM(s) and the corresponding cloudlet(s) for these VM(s)
         *  set VM ids for those cloudlets and add them to such service
         *  add service id and the corresponding VM id list
         *  set stream dependencies info. 
         *  add input and output streams for the cloudlets of this service
        *
        **/
	@Override
        public void doScheduling(LinkedHashMap<Integer, VMOffers> datacentersWithVMOffers) 
        {
		Enumeration<Stream> dataIter = dataItems.elements();
		while(dataIter.hasMoreElements()){
			Stream item = dataIter.nextElement();
			streamRequiredLocation.put(item.getId(), new HashSet<Integer>());
		}
		
		//===== 1.Determine available computation services =====
                //Generate scheduling plan
                LinkedHashMap<Integer, ArrayList<Integer>> ServiceVMsMap= new LinkedHashMap<>();
                LinkedHashMap<Integer, Integer> ServiceSelectedDatacenterMap= new LinkedHashMap<>(); 
                
                Random generator=new Random(provisioningSeed);
                  
                for(Service service: services)
                {
                    Integer serviceID = service.getId();
                    
                    int datacenterStartIndex = 2; //datacenter id starts from 2 due to CloudSim assigned ids for datacenter entities starting from 2; 
                    
                    ArrayList<Integer> selectedVMs=new ArrayList<Integer>();
                    double requiredMIPS=0;
                    if(service.getUserDataProcessingRateReq()!=-1)
                    {
                        double totalIn=service.getTotalSizeOfServiceInputStreams();
                        if(service.getUserDataProcessingRateReq() >= totalIn)
                            requiredMIPS=service.getUserDataProcessingRateReq() *service.getDataProcessingReq();
                        else
                            requiredMIPS=totalIn *service.getDataProcessingReq();
                        
                    }
                    else
                        requiredMIPS=service.getTotalSizeOfServiceInputStreams() *service.getDataProcessingReq();
                    
                    
                    int randomDatacenter = generator.nextInt(datacentersWithVMOffers.size());
                    
                    int selectedDatacenterID= datacenterStartIndex+ randomDatacenter;
                    
                    //Add service id and selected datacenter (random picked one) to map
                    ServiceSelectedDatacenterMap.put(serviceID, selectedDatacenterID);
                    
                    VMOffers vmOffers=datacentersWithVMOffers.get(selectedDatacenterID);
                    Set setVms= vmOffers.getVmOffers().keySet();
                    
                    ArrayList<Vm> vms = new ArrayList<Vm>(setVms);
                    
                    
                    boolean toProviionVM=false;
                    for(int i=0; i< vms.size(); i++)
                    {
                        double vmMIPS=vms.get(i).getMips() * vms.get(i).getNumberOfPes(); //multiple MIPS by number of PEs (because it  is an VMOffer, where after VM created, it is possible to use vm.getCurrentRequestedTotalMips())
                        
                        if(vmMIPS/service.getDataProcessingReq()< minDPUnit)
                            continue;
                                                
                        if(vmMIPS <= requiredMIPS)
                        {
                           if(i+1<vms.size())
                           {
                               double nextVmMIPS=vms.get(i+1).getMips() * vms.get(i+1).getNumberOfPes(); //multiple MIPS by number of PEs (because it  is an VMOffer, where after VM created, it is possible to use vm.getCurrentRequestedTotalMips())
                               if(nextVmMIPS>requiredMIPS) //to provision ith VM
                                {
                                    toProviionVM=true;
                                }
                           }
                           else
                           {
                               selectedVMs.add(vms.get(i).getId());
                               requiredMIPS= requiredMIPS - vmMIPS;
                               i--;
                           }
                        }
                        else //vmMIPS > requiredMIPS
                        {
                            if(i-1>=0)
                            {
                                double previousVmMIPS=vms.get(i-1).getMips() * vms.get(i-1).getNumberOfPes(); //multiple MIPS by number of PEs (because it  is an VMOffer, where after VM created, it is possible to use vm.getCurrentRequestedTotalMips())
                                if(previousVmMIPS>=requiredMIPS && previousVmMIPS<vmMIPS && previousVmMIPS/service.getDataProcessingReq()>= minDPUnit)
                                {
                                    i=i-2;
                                }
                                else
                                {
                                    toProviionVM=true;
                                }
                            }
                            else //reach last vm i==0, so just provision it
                            {
                                    toProviionVM=true;
                            }

                        }
                        
                        if(toProviionVM)
                        {
                            selectedVMs.add(vms.get(i).getId());
                            requiredMIPS= requiredMIPS - vmMIPS;                            
                            toProviionVM=false;
                        }
                        
                        if(requiredMIPS<=0)
                            break;
                    }
                    
                    if(selectedVMs.isEmpty()) // in case of no VM offer in selected cloud-based datacenter that achieves the required MIPS for processing minimum stream unit
                    {
                        JOptionPane.showMessageDialog(null, "IoTSim-Stream will be terminated because provisioning VM(s) for Service " + serviceID + " is not possible with available VM offer(s)\n" +"Reason: there is no VM offer in selected cloud-based datacenter that achieves the required MIPS for processing one stream unit (i.e. " + (minDPUnit * service.getDataProcessingReq())+ " MIPS)");
                        Log.print("IoTSim-Stream is terminated because provisioning VM(s) for Service " + serviceID + " is not possible with available VM offer(s)\nReason: " +"there is no VM offer in selected cloud-based datacenter that achieves the required MIPS for processing one stream unit (i.e. " + (minDPUnit * service.getDataProcessingReq())+ " MIPS)");
                        System.out.println("IoTSim-Stream is terminated because provisioning VM(s) for Service " + serviceID + " is not possible with available VM offer(s)\nReason: " +"there is no VM offer in selected cloud-based datacenter that achieves the required MIPS for processing one stream unit (i.e. " + (minDPUnit * service.getDataProcessingReq())+ " MIPS)");
                        System.exit(0);
                    }
                        
                    ServiceVMsMap.put(serviceID, selectedVMs);
                    
                    double sum=0.0;
                    for(Integer vmid: selectedVMs)
                    {
                        sum+=datacentersWithVMOffers.get(selectedDatacenterID).getVM(vmid).getMips() * datacentersWithVMOffers.get(selectedDatacenterID).getVM(vmid).getNumberOfPes();
                    }
                }
                                
                
		//Fast and simple solution found, now creating scheduling plan (provision VM + create cloudlets of each service + set relative processing percent for cloudlets of each service + set stream dependencies info and add input&output required streams to cloudlets)
                for(Integer serviceID:ServiceVMsMap.keySet()) //for each service (service chromosome)
                {
                    //use VMs that satisfy performance req., one cloudlet per VM
                    Service service=services.get(serviceID);
                    double serviceSize=service.getDataProcessingReq();  // Service data processing reuqirement - Milion instruction per KB 
                    ArrayList<Integer> vmidList = new ArrayList<Integer>();    
                    int placementDatacenterID=ServiceSelectedDatacenterMap.get(serviceID); //the same for all selected vms since we provisioned all vms for a service from one cloud
                    for(Integer vmid: ServiceVMsMap.get(serviceID)) //for each vm; it must be using ServiceSelectedDatacenterMap to get corresponding datacenterid for this vmid
                    {    
                        Vm instance = datacentersWithVMOffers.get(placementDatacenterID).getVM(vmid); //cloud provider instance
                        double vmCost=datacentersWithVMOffers.get(placementDatacenterID).getVmOffers().get(instance);

                        SVM newVm = new SVM(vmId,ownerId,instance.getMips(),instance.getNumberOfPes(),instance.getRam(),instance.getBw(),instance.getSize(),"", new ServiceCloudletSchedulerSpaceShared());
                        provisioningInfo.add(new ProvisionedSVm(newVm,0,0,vmCost,placementDatacenterID));
                        
                        //Create new ServiceCloudlet for this vm
                        ServiceCloudlet cl=new ServiceCloudlet(cloudletCont,1,newVm.getNumberOfPes(),0,0,new UtilizationModelFull(),new UtilizationModelFull(),new UtilizationModelFull(),serviceSize,ownerId,serviceID);
                        cl.setVmId(newVm.getId());
                        service.addCloudlet(cl);
                        vmidList.add(newVm.getId());
                        cloudletCont++;
                        vmId++;
                    }
                    
                    schedulingTable.put(service.getId(), vmidList);

                    //set stream dependencies info
                    for(Stream stream:service.getStreamDependencies()){
                            if(!streamRequiredLocation.containsKey(stream.getId())){
                                    streamRequiredLocation.put(stream.getId(), new HashSet<Integer>());
                            }
                            // entry stream id and at which service such stream should be avaliable
                            streamRequiredLocation.get(stream.getId()).add(service.getId());
                            
                            //add input stream to cloudlet requiredInputStream list
                            for(ServiceCloudlet cloudlet: service.getServiceCloudlets())
                                cloudlet.addRequiredInputStream(stream);
                    }

                    //add input & output(next loop) stream to service clouodlets
                    for(Stream stream:service.getOutput()){
                            if(!streamRequiredLocation.containsKey(stream.getId())){
                                    streamRequiredLocation.put(stream.getId(), new HashSet<Integer>());
                            }
                            
                            //add output stream to cloudlet requiredOutputStream list
                            for(ServiceCloudlet cloudlet: service.getServiceCloudlets())
                                cloudlet.addProducedOutputStream(stream);
                    }
		}
	}
}
