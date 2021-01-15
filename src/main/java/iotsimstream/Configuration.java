package iotsimstream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class implements the properties manager. Here the
 * name of the properties file is defined. Moreover, when
 * properties are to be read, this is the class that is called.
 *
 */
public enum Configuration {
	
	INSTANCE;
		
	private static Properties properties = System.getProperties();
    
	static {
            try {
                //Properties file in resources package
                FileInputStream propfile = new FileInputStream("src/main/java/iotsimstream/resources/simulation.properties");
                properties.load(propfile);
            } catch (IOException ex) {
                Logger.getLogger(Configuration.class.getName()).log(Level.SEVERE, null, ex);
            }
	}
		 
    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    
    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }
}
