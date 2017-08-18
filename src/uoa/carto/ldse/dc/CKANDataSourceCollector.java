package uoa.carto.ldse.dc;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.jena.atlas.web.HttpException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import uoa.carto.ldse.interfaces.DBInterface;
import uoa.carto.ldse.interfaces.TriplesInterface;



/**
 * @author vkopsachilis
 *
 */

// Collects Data Sources and their SPARQL or RDF Resources from CKAN Data Catalogs        	
public class CKANDataSourceCollector {
 
	//Variable to define if program should also collect spatial classes
	public static boolean collectSpatialClasses=false;
    // Main Method
	public static void main(String[] args) throws IOException, HttpException, ParseException, SQLException{
 		 	 
		 //Get CKAN Catalogs from Database
		 List<String> catalogs=DBInterface.getCKANCatalogs();
		 
		 try  {	
			 //Iterate Through Catalogs
			 for (int c=1; c<=catalogs.size();c++){	
				 readCKANCatalog(catalogs.get(c-1),c);							 										
			 }//end for catalogs
		 
		 }catch (Exception e){
			 System.out.println("EXCEPTION: "+ e);
		 }

	}//End of Main
	
	// Read a CKAN Catalog. Parses all the packages serially (parses 20 packages per request. Otherwise hangs...)
	public static void readCKANCatalog(String catalog,int catalogID) throws ClientProtocolException, IOException, ParseException, SQLException{
		HttpPost request;
		HttpResponse response;
		String jsonString;
		Object obj ;
		JSONObject json;
		JSONArray results;
		HttpClient httpClient = HttpClientBuilder.create().build();
		JSONParser parser = new JSONParser();
		int offset=0;
		boolean endOfDatasets=false;
			
		while  (endOfDatasets==false){ // While request return packages...
			request = new HttpPost(catalog+"/api/3/action/current_package_list_with_resources?limit=20&offset="+offset);
			//Navigational println
			System.out.println(offset);
			offset=offset+20;		
			// JSON parsing ...
			request.addHeader("content-type", "application/json");			
			response = httpClient.execute(request);		
			jsonString = EntityUtils.toString(response.getEntity(), "UTF-8"); 
			obj = parser.parse(jsonString);  
			json = (JSONObject) obj;
			results = (JSONArray) json.get("result");
				
			if (results.size()>0){ 
				Iterator<JSONObject> pack_iterator = results.iterator();
				//Iterate through  packages
				while (pack_iterator.hasNext()) { 
					// Parse a Data Source
				 	readDataSource(pack_iterator.next(),catalogID);		 
				 }
			}else{
				// If no more data sources then signal it
				endOfDatasets=true;
			}	// end if
		}	// end while
			 
	} // End readCKANCatalog  
	    
	// Read a Data Source
	public static void readDataSource(JSONObject pack,int catalogID) throws SQLException{
	    
		// Get its title and resources 
	 	String datasourceTitle=(String)pack.get("title");
	 	JSONArray resources = (JSONArray) pack.get("resources"); 
	 	Iterator<JSONObject> resource_iterator = resources.iterator(); 
	 	
	 	//Iterate though package resources
		while (resource_iterator.hasNext()) {
			parseDataSourceResource(resource_iterator.next(),datasourceTitle,catalogID);
		}
		
	 } // End readDataSource
	    
	// Read a resource (e.g. SPARQL endpoint or RDF dump)
	public static void parseDataSourceResource(JSONObject resource, String title, int catalogID) throws SQLException{
		
		// Get its format and url	 
		String datasourceFormat=(String) resource.get("format");
		String datasourceURL=(String) resource.get("url");
		String status;
		
		//check the format of the resourse (e.g. SPARQL, RDF, turtle etc)
		if (datasourceFormat.toLowerCase().indexOf("sparql")>-1) {
			System.out.println(title +"............."+datasourceFormat+"("+datasourceURL+")");
			// parse the SPARQL endpoint
			status=TriplesInterface.checkIfSPARQLHasSpatialOntologies(datasourceURL);
			//Update the database
			DBInterface.updateDatasourcesInDB(catalogID,title,datasourceURL,1,status);
		    if (collectSpatialClasses){
		    	if (status.equals("spatial")) DatasetCollector.parseSPARQLDataSourceSpatialClasses(datasourceURL);
		    }
		} else if (datasourceFormat.toLowerCase().indexOf("rdf")>-1) {
			 //status=parseRDF(datasourceURL);
		
		} else{
			 //Do Nothing
		} // End if
			 
	 } //End parseDataSourceResource
	
	
	
}//End of Class
