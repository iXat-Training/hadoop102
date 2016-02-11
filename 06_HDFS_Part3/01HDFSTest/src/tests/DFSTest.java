package tests;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DFSTest {

	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		//set/ override the default configuration here
	   //conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
	    
		//also, you can add core-site.xml to the classpath
	    
		//or you could also set a URI on the FileSystem.get to point the code to a cluster
		
		FileSystem fs = FileSystem.get(new URI("hdfs://127.0.0.1:8020"),conf);
	  
		//if you construct a FileSystem object operates in LocalMode if there is no fs.defaultFS set or if there is no hdfs uri set on FileSystem class
		//FileSystem fs = FileSystem.get(conf);
		  
		Path filenamePath = new Path("/a.txt");
	    if (fs.exists(filenamePath)) {
	        fs.delete(filenamePath);
	     }

	    FSDataOutputStream out = fs.create(filenamePath);
	    out.writeBytes("Hello, this is some content from a java program");
	    out.close();
	    System.out.println("File written.... now reading the file");
	    FSDataInputStream in = fs.open(filenamePath);
	    String messageIn = in.readLine();
	    System.out.print(messageIn);
	    in.close();


	}

}
