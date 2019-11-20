/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package iotsimstream;

import java.util.ArrayList;

/**
 *
 * @author Mutaz Barika
 */
public class GraphAppClouldlet {
    
    ArrayList<Service> services;

    public GraphAppClouldlet(ArrayList<Service> services) {
        this.services = services;
    }
    
    public Service getServiceAtIndex(int index)
    {
        return services.get(index);
    }
    
    public Service getServiceByID(int serviceid)
    {
        for(Service service: services)
            if(service.getId() == serviceid)
            return service;
        
        return null;
    }
    
}
