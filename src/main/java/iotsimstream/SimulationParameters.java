/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package iotsimstream;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 *
 * @author Mutaz Barika
 */
public class SimulationParameters {
 
    
    static ArrayList<String> ApplicationsForExp1 = new ArrayList<>(Arrays.asList("App1","App2","App3"));
    static ArrayList<String> App1Sizes = new ArrayList<>(Arrays.asList("App1_VerySmall","App1_Small","App1_Medium","App1_Large", "App1_VeryLarge", "App1_DoubleLarge"));
    static ArrayList<String> App2Sizes = new ArrayList<>(Arrays.asList("App2_VerySmall","App2_Small","App2_Medium","App2_Large", "App2_VeryLarge", "App2_DoubleLarge"));
    static ArrayList<String> App3Sizes = new ArrayList<>(Arrays.asList("App3_VerySmall","App3_Small","App3_Medium","App3_Large", "App3_VeryLarge", "App3_DoubleLarge"));
    static ArrayList<String> AppsMediumSize = new ArrayList<>(Arrays.asList("App1_Medium","App2_Medium","App3_Medium"));
    static ArrayList<String> AppsSmallSize = new ArrayList<>(Arrays.asList("App1_Small","App2_Small","App3_Small"));
    static ArrayList<String> AppsLargeSize = new ArrayList<>(Arrays.asList("App1_Large","App2_Large","App3_Large"));
    static ArrayList<String> AppsVeryLargeSize = new ArrayList<>(Arrays.asList("App1_VeryLarge","App2_VeryLarge","App3_VeryLarge"));
    static ArrayList<String> AppsDoubleLargeSize = new ArrayList<>(Arrays.asList("App1_DoubleLarge","App2_DoubleLarge","App3_DoubleLarge"));
    
    static Random rngEXSource;
    static Random rngOutputDataRate; //this rate is calculated suing output proportion ranges form input
    static Random rngServiceDPReq;
    static Random rngServiceUserDPRate;
    static Random rngIngressBandwidth;
    static Random rngIngressLatency;
    static Random rngEgressBandwidth;
    static Random rngEgressLatency;
    static Random rngDTCOST;
    static Random rngMinDPRate;
    static Random rngServiceMovable;
    static int scenario;
    static int serviceCount;
    
    

    public SimulationParameters(int scenario) {

        this.scenario=scenario;
    }
    

    public static ArrayList<String> getApplicationsForExp1() { //Scenario 1 and 2
        return ApplicationsForExp1;
    }

    public static ArrayList<String> getApp1Sizes() {
        return App1Sizes;
    }

    public static ArrayList<String> getApp2Sizes() {
        return App2Sizes;
    }
    
    public static ArrayList<String> getApp3Sizes() {
        return App3Sizes;
    }
    
    public static ArrayList<String> getAppsSmallSize() {
        return AppsSmallSize;
    }
    
    public static ArrayList<String> getAppsMediumSize() {
        return AppsMediumSize;
    }
    
    public static ArrayList<String> getAppsLargeSize() {
        return AppsLargeSize;
    }
    
    public static ArrayList<String> getAppsVeryLargeSize() {
        return AppsVeryLargeSize;
    }
    
    
    public static ArrayList<String> getAppsDoubleLargeSize() {
        return AppsDoubleLargeSize;
    }
}

