/**
 * 
 */
package uoa.carto.ldse.dc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import uoa.carto.ldse.interfaces.DBInterface;
import uoa.carto.ldse.interfaces.TriplesInterface;

/**
 * @author vkopsachilis
 *
 */
public class DatasetCollector {

	/**
	 * @param args
	 */
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 List<String> spatialResources;
		try {
			//Get Spatial Resources from DB
			spatialResources = DBInterface.getSpatialResources();
		 
		 //Iterate Through Spatial Resources
		 for (int c=1; c<=spatialResources.size();c++){	
			 //If Sparql
			String datasourceURL=spatialResources.get(c-1);
			//If Resource Type is SPARQL
			if (DBInterface.getResourceTypeByURL(datasourceURL)==1) {
				parseSPARQLDataSourceSpatialClasses(datasourceURL); 	
			}
			 
		 }//end for resources
		 
		 DBInterface.finalizeDatasetCollector();
		 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		 }		   
	}
	
	public static void parseSPARQLDataSourceSpatialClasses(String datasourceURL){
		parseSPARQLDataSourceSpatialClassesByOntology(datasourceURL,1);
    	parseSPARQLDataSourceSpatialClassesByOntology(datasourceURL,2);
    	parseSPARQLDataSourceSpatialClassesByOntology(datasourceURL,3);
    	parseSPARQLDataSourceSpatialClassesByOntology(datasourceURL,7);
    	parseSPARQLDataSourceSpatialClassesByOntology(datasourceURL,10);
    	parseSPARQLDataSourceSpatialClassesByOntology(datasourceURL,16);	
	}
	
	public static void parseSPARQLDataSourceSpatialClassesByOntology(String resource_url, int ontology_id){
		List<String> spatialClasses=TriplesInterface.getSPARQLSpatialClasses(resource_url,TriplesInterface.getSpatialClassesQuery(ontology_id));
		System.out.println(spatialClasses.size()+ " classes found for ontology "+ ontology_id);
		List<String> classGeometries=new ArrayList<String>();
		 try  {	
			 //Iterate Through Spatial Datasets
			 for (int s=1; s<=spatialClasses.size();s++){	
				 System.out.println("Getting geometries from "+s+"th class " + spatialClasses.get(s-1));	 
				 int geometryClassCount=TriplesInterface.getSPARQLCountClassGeometries(resource_url, TriplesInterface.getCountClassGeometiesQuery(spatialClasses.get(s-1), ontology_id));
				 if ((geometryClassCount>5)  && (geometryClassCount<100000)){ 
					 DBInterface.updateDatasetsinDB(spatialClasses.get(s-1),DBInterface.getResourceID(resource_url),ontology_id,geometryClassCount);
				 }
			 }	 
		 }catch (Exception e){
			 System.out.println("EXCEPTION: "+ e);		
		 }
		
	}

}
