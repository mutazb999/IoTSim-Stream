package iotsimstream;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.Vm;

/**
 * This abstract contains encapsulates VM options offered
 * by IaaS providers. Its methods return instance types offered,
 * cost of different instances and the leasing time slot.
 *
 */
public abstract class VMOffers {
	
	LinkedHashMap<Vm,Double> vmOffersTable=new LinkedHashMap<>();
	
	/**
	 * Returns the instances offered by the provider, and the respective
	 * prices per time slot (in cents).
	 */
	public abstract LinkedHashMap<Vm,Double> getVmOffers();
	
	/**
	 * Returns the cost of a specific instance in cents per second.
	 */
        public abstract double getCost(double mips, int pes, int memory,long bw);
        //public abstract int getCost(Vm vm);
	//
	
	/**
	 * Returns the duration of the lease time slot in seconds.
	 */
	//public abstract long getTimeSlot();
	
	/**
	 * Returns the average boot time of a VM in seconds.
	 */
	public abstract long getBootTime();

	public VMOffers(){
		vmOffersTable = new LinkedHashMap<Vm,Double>();
	}
        
        public Vm getVM(int vmid)
        {
            for(Vm vm: vmOffersTable.keySet())
            {
                if(vm.getId()==vmid)
                    return vm;
            }
            return null;
        }
}
