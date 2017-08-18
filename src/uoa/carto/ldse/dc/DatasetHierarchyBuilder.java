/**
 * 
 */
package uoa.carto.ldse.dc;

import java.sql.SQLException;
import java.util.List;

import uoa.carto.ldse.interfaces.DBInterface;
import uoa.carto.ldse.interfaces.TriplesInterface;

/**
 * @author vkopsachilis
 *
 */
public class DatasetHierarchyBuilder {

	/**
	 * @param args
	 */
	//Keep Hierarchy Info for the availiable classes
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		 List<Integer> datasetResourceIds;
		try {
			datasetResourceIds = DBInterface.getDatasetResourceIds();
			//Iterate Through Spatial Resources
			for (int r=1; r<=datasetResourceIds.size();r++){	
				String resource_url=DBInterface.getResourceUrlById(datasetResourceIds.get(r-1));
				System.out.println("Parsing: "+resource_url);
				List<String> resourceDatasets=DBInterface.getResourceDatasets(datasetResourceIds.get(r-1));				
				List<Integer> resourceDatasetsIds=DBInterface.getResourceDatasetsIds(datasetResourceIds.get(r-1));
				//Iterate Through A resource Datasets
				for (int c=1; c<=resourceDatasets.size();c++){
					System.out.println("Finding Parents for: "+ resourceDatasets.get(c-1));
					if (resourceDatasetsIds.get(c-1)>5200){
					for (int p=1; p<=resourceDatasets.size();p++){
						boolean isChild=false;
						try{
						isChild=TriplesInterface.AskSparqlQuery(resource_url, "ASK {<"+resourceDatasets.get(c-1)+"> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <"+resourceDatasets.get(p-1)+">}");
						}catch (Exception e){
							 System.out.println("EXCEPTION: "+ e);
						}
						if (isChild){ 
							System.out.println("PARENT: "+ resourceDatasets.get(p-1));
							DBInterface.addHierarchy(resourceDatasetsIds.get(c-1),resourceDatasetsIds.get(p-1));
						}
					}
				}}
			}
		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}//End Main

}//End Class
