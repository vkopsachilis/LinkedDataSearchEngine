package uoa.carto.ldse.interfaces;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Ontology Definitions
 *      W3cBasicGeo 		- "ASK {?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?o}"	
 							- "ASK {?s <http://www.w3.org/2003/01/geo/wgs84_pos#geometry> ?o}"
		NeoGeo      		- "ASK {?s <http://geovocab.org/geometry#geometry> ?o}"
		              		- "ASK {?s ?p <http://geovocab.org/geometry#Geometry>}"
		GeoSparql    		- "ASK {?s <http://www.opengis.net/ont/geosparql#asWKT> ?o}" 
		             		- "ASK {?s <http://www.opengis.net/ont/geosparql#asGML> ?o}" 	
		StSparql      		- "ASK {?s <http://strdf.di.uoa.gr/ontology#geometry> ?o}"
		GML          		- "ASK {?s <http://www.opengis.net/gml/Point> ?o}"
		              		- "ASK {?s <http://www.opengis.net/gml/pos> ?o}"	
		GeoRSS       		- "ASK {?s <http://www.georss.org/georss/point> ?o}	"
		Geonames    		- "ASK {?s <http://www.geonames.org/ontology#featureCode> ?o}"
		              		- "ASK {?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?o}"
		LinkedGeodata 		- down
		Simple Features 	- down
		OrdanceSurvey   	- "ASK {?s <http://data.ordnancesurvey.co.uk/ontology/geometry/asGML> ?o}"
		ISA Programme Location Core Vocabulary  -"ASK {?s <http://www.w3.org/ns/locn#geometry> ?o}"
		IGN France 			- french
		Places Ontology 	- down
		Geofeatures Ontology -down
		FAO Ontology 		- polygons
 * 
 **/

// Class for Interaction with triples 
public class TriplesInterface {
	
	//Ask for spatial predicates queries
	public static String askforWGS84			="ASK {?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?o}";
	public static String askforNeoGeo			="ASK {?s <http://geovocab.org/geometry#geometry> ?o}";
	public static String askforGeoSparqlWKT		="ASK {?s <http://www.opengis.net/ont/geosparql#asWKT> ?o}";
	public static String askforGeoSparqlGML		="ASK {?s <http://www.opengis.net/ont/geosparql#asGML> ?o}";
	public static String askforStSparql			="ASK {?s <http://strdf.di.uoa.gr/ontology#geometry> ?o}";
	public static String askforGML				="ASK {?s <http://www.opengis.net/gml/pos> ?o}";
	public static String askforGeoRSS			="ASK {?s <http://www.georss.org/georss/point> ?o}";
	public static String askforOS				="ASK {?s <http://data.ordnancesurvey.co.uk/ontology/geometry/asGML> ?o}";
	public static String askforCoreLocation		="ASK {?s <http://www.w3.org/ns/locn#geometry> ?o}";
	public static String askforWGS84geometry	="ASK {?s <http://www.w3.org/2003/01/geo/wgs84_pos#geometry> ?o}";
	
	
	public static String getWGS84Classes		="SELECT DISTINCT ?t WHERE {?s <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s a ?t}";
	public static String getNeoGeoClasses		="SELECT DISTINCT ?t WHERE {?s <http://geovocab.org/geometry#geometry> ?o. ?o <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?s a ?t}";
	public static String getGeoSparqlClasses	="SELECT DISTINCT ?t WHERE {?s <http://www.opengis.net/ont/geosparql#hasGeometry> ?o. ?o <http://www.opengis.net/ont/geosparql#asWKT> ?wkt. ?s a ?t. filter(regex(lcase(?wkt), 'point' ))}";
	public static String getStSparqlClasses		="";
	public static String getGMLClasses			="SELECT DISTINCT ?t WHERE {?s <http://www.opengis.net/gml/Point> ?o. ?o <http://www.opengis.net/gml/pos> ?p. ?s a ?t}";
	public static String getGeonamesClasses		="SELECT DISTINCT ?t WHERE {?s <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://www.geonames.org/ontology#featureClass> ?t}";
	public static String getOSClasses			="SELECT DISTINCT ?t WHERE {?s <http://data.ordnancesurvey.co.uk/ontology/geometry/extent> ?o. ?o <http://data.ordnancesurvey.co.uk/ontology/geometry/asGML> ?gml. ?o a ?t}";
	public static String getGeoRSSClasses		="SELECT DISTINCT ?t WHERE {?s <http://www.georss.org/georss/point> ?o. ?s a ?t}";
	public static String getCoreLocationClasses	="";
	public static String getWGS84GeomClasses	="SELECT DISTINCT ?t WHERE {?s <http://www.w3.org/2003/01/geo/wgs84_pos#geometry> ?g. ?g <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?l. ?s a ?t}";
	
	
	
	 // Performs an Ask Sparql Query to an endpoint
    public static boolean AskSparqlQuery(String datasource, String query){
		QueryExecution qexec = QueryExecutionFactory.sparqlService(datasource, QueryFactory.create(query));
		qexec.setTimeout(18000);	
		return qexec.execAsk();
	} // End AskSparqlQuery
    
    // Performs an Select Sparql Query to an endpoint
    public static ResultSet SelectSparqlQuery(String datasource, String query){
		QueryExecution qexec = QueryExecutionFactory.sparqlService(datasource, QueryFactory.create(query));
		qexec.setTimeout(18000);	
		return qexec.execSelect();
	} // End SelectSparqlQuery
    
	
    
	// Parsing SPARQL Endpoint to check if has spatial predicates
	public static String checkIfSPARQLHasSpatialOntologies(String url){
    	//SPARQL Endpoint status. Can be:
		// 1. spatial    --> has Spatial Features
		// 2. no spatial --> has no spatial features
		// 3. error msg  --> an error occured while parsing
    	String status="no spatial";
    	
    	try{
    		 //check if has spatial predicates
    		 if (AskSparqlQuery(url,askforWGS84))         		status="spatial";
    		 else if (AskSparqlQuery(url,askforGeoRSS))  		status="spatial"; 
    		 else if (AskSparqlQuery(url,askforWGS84geometry))  status="spatial"; 
    		 else if (AskSparqlQuery(url,askforNeoGeo))  		status="spatial"; 
    		 else if (AskSparqlQuery(url,askforGeoSparqlWKT))  	status="spatial"; 
    		 
    	}catch (QueryException e){
			 System.out.println("QUERYEXCEPTION: "+ e);
			 status=e.toString();
		 }catch (Exception e){
			 System.out.println("EXCEPTION: "+ e);
			 status=e.toString();
		 }
    	return status;
    } // End parseSPARQL
    
	
	// Parsing RDF Dump to check if has spatial predicates
	//TODO
    public static String checkIfRDFHasSpatialOntologies(String url){
    	return "";
    } //End checkIfRDFHasSpatialOntologies
    
     
  
    
    
    public static List<String> getSPARQLSpatialClasses(String resource_url,String query){
    	List<String> spatialClasses=new ArrayList<String>();
		ResultSet results =null;
		try{
			results=SelectSparqlQuery(resource_url, query);
			for (;results.hasNext();){
				spatialClasses.add(results.nextSolution().getResource("t").getURI());	
			}
		}catch (QueryException e){
			 System.out.println("QUERYEXCEPTION: "+ e);
		}catch(Exception e){
			System.out.println("EXCEPTION: "+ e);	
		}
    	return spatialClasses;
    }
    
    
    public static String getSpatialClassesQuery(int ontology){
  		String query="";
  		switch (ontology){
  			case 1: query= getWGS84Classes; break; //W3C Basic Geo
  			case 2: query= getNeoGeoClasses; break; //NeoGeo
  			case 3: query= getGeoSparqlClasses; break; //GeoSparql
  			case 4: query= getStSparqlClasses; break; //StSparql
  			case 5: query= getGMLClasses; break; //GML  		
  			case 7: query= getGeonamesClasses; break; //Geonames  		
  			case 9: query= getOSClasses; break; //OrdanceSurvey 		
  			case 10: query=getGeoRSSClasses; break; //GeoRSS  		
  			case 11: query=getCoreLocationClasses; break;	//ISA	  		
  			case 16: query=getWGS84GeomClasses; break;	//W3C Basic Geo Geometry	
  		}	
  		
  		return query;
  	}
    
  
    public static List<String> returnClassGeometries(String resource_url, String dataset, int ontology_id){
		 List<String> classGeometries= null;
		 System.out.println("Getting geometries from " + dataset);
		 int geometryClassCount=TriplesInterface.getSPARQLCountClassGeometries(resource_url, TriplesInterface.getCountClassGeometiesQuery(dataset, ontology_id));
		 System.out.println("Features Count:"+geometryClassCount);
		 if ((geometryClassCount>5)  && (geometryClassCount<100000)){
			 classGeometries=TriplesInterface.getSPARQLClassGeometries(resource_url, TriplesInterface.getClassGeometiesQuery(dataset, ontology_id));
		 }
	return classGeometries;
	}
   
    
    public static List<String> getSPARQLClassGeometries(String resource_url,String query){
    	List<String> classGeometries=new ArrayList<String>();
    
		ResultSet results =null;
		QuerySolution sol;
		try{
			results=SelectSparqlQuery(resource_url, query);
			for (;results.hasNext();){
				sol=results.nextSolution();
				//TODO implementation for xy and wkt
				if (sol.contains("x")){
					classGeometries.add(parseXY(sol.getLiteral("x").getString(),sol.getLiteral("y").getString()));	
				}else if (sol.contains("wkt")){
					classGeometries.add(parseWKT(sol.getLiteral("wkt").getString()));	
				}else if (sol.contains("pos")){
					classGeometries.add(parsePOS(sol.getLiteral("pos").getString()));	
				}else {
					System.out.println(sol.getLiteral("gml").getString());
				}
			}
		 }catch (QueryException e){
			 System.out.println("QUERYEXCEPTION: "+ e);
		 }catch (Exception e){
			 System.out.println("EXCEPTION: "+ e);
		 }
    	return classGeometries;
    }
    
    //For ontologies W3CBasicGeo, NeoGeo, Geonames
    public static String parseXY(String x, String y){
    	return x + "," + y ;
    }
    //For ontologies GeoSPARQL
    public static String parseWKT(String wkt){
    	String x=wkt.substring(wkt.indexOf("POINT(")+6, wkt.lastIndexOf(" "));
		String y=wkt.substring(wkt.lastIndexOf(" ")+1, wkt.lastIndexOf(")"));
    	return  x + "," + y;
    }
    //For ontologies GeoRSS, GML
    public static String parsePOS(String pos){
    	String y=pos.substring(0, pos.lastIndexOf(" "));
		String x=pos.substring(pos.lastIndexOf(" ")+1, pos.length());
    	return x + "," + y ;
    }
    
    //Getting Class geometries query
    public static String getClassGeometiesQuery(String type,int ontology){
		String query="";
		switch (ontology){
		//W3C Basic Geo
		case 1: query="SELECT DISTINCT ?x ?y  WHERE {?s <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">}"; break;
		//NeoGeo
		case 2: query="SELECT DISTINCT ?x ?y WHERE {?o <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?o <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://geovocab.org/geometry#geometry> ?o . ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">}"; break;
		//GeoSparql
		case 3: query="SELECT DISTINCT ?wkt WHERE {?s <http://www.opengis.net/ont/geosparql#hasGeometry> ?o. ?o <http://www.opengis.net/ont/geosparql#asWKT> ?wkt. ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">.} "; break;
		//StSparql
		case 4: query=""; break;
		//GML
		case 5: query="SELECT DISTINCT ?pos WHERE {?s <http://www.opengis.net/gml/Point> ?o. ?o <http://www.opengis.net/gml/pos> ?pos. ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">} "; break;
		//Geonames
		case 7: query="SELECT DISTINCT ?x ?y  WHERE {?s <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://www.geonames.org/ontology#featureClass> <"+type+">} "; break;
		//OrdanceSurvey
		case 9: query="SELECT DISTINCT ?gml WHERE {?s <http://data.ordnancesurvey.co.uk/ontology/geometry/extent> ?o. ?o <http://data.ordnancesurvey.co.uk/ontology/geometry/asGML> ?gml. ?o a <"+type+">} "; break;
		//GeoRSS
		case 10: query="SELECT DISTINCT ?pos WHERE {?s <http://www.georss.org/georss/point> ?pos. ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">} "; break;
		//ISA
		case 11: query=""; break;
		//W3C Basic Geo Geometry
    	case 16: query="SELECT DISTINCT ?x ?y WHERE {?g <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?g <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://www.w3.org/2003/01/geo/wgs84_pos#geometry> ?g. ?s a <"+type+">} "; break;		
		
		}	
		return query;
	}
    
    //Get the number of the features in the Class
    public static int getSPARQLCountClassGeometries(String resource_url,String query){
    	int classGeometriesCount=0;
    	try{
    	
    		QuerySolution sol;
    		ResultSet results=SelectSparqlQuery(resource_url, query);
    		for (;results.hasNext();){
    			sol=results.nextSolution();
    			classGeometriesCount=sol.getLiteral("c").getInt();			
    		}
    	
    	}catch (QueryException e){
			 System.out.println("QUERYEXCEPTION: "+ e);
		}catch (Exception e){
			 System.out.println("EXCEPTION: "+ e);
		}
		
    	return classGeometriesCount;
    }//End getSPARQLCountClassGeometries
    
	//Getting the number of a class geometries query
	public static String getCountClassGeometiesQuery(String type,int ontology){
		String query="";
		switch (ontology){
		//W3C Basic Geo
		case 1: query="SELECT (count(DISTINCT ?x) as ?c)  WHERE {?s <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">}"; break;
		//NeoGeo
		case 2: query="SELECT (count(DISTINCT ?x)  as ?c) WHERE {?o <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?o <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://geovocab.org/geometry#geometry> ?o . ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">}"; break;
		//GeoSparql
		case 3: query="SELECT (count(DISTINCT ?wkt) as ?c) WHERE {?s <http://www.opengis.net/ont/geosparql#hasGeometry> ?o. ?o <http://www.opengis.net/ont/geosparql#asWKT> ?wkt. ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">.}"; break;
		//StSparql
		case 4: query=""; break;
		//GML
		case 5: query="SELECT (count(DISTINCT ?pos) as ?c) WHERE {?s <http://www.opengis.net/gml/Point> ?o. ?o <http://www.opengis.net/gml/pos> ?pos. ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">}"; break;
		//Geonames
		case 7: query="SELECT (count(DISTINCT ?x) as ?c)  WHERE {?s <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?x. ?s <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://www.geonames.org/ontology#featureClass> <"+type+">}"; break;
		//OrdanceSurvey
		case 9: query="SELECT (count(DISTINCT ?gml) as ?c) WHERE {?s <http://data.ordnancesurvey.co.uk/ontology/geometry/extent> ?o. ?o <http://data.ordnancesurvey.co.uk/ontology/geometry/asGML> ?gml. ?o a <"+type+">}"; break;
		//GeoRSS
		case 10: query="SELECT (count(DISTINCT ?pos)  as ?c) WHERE {?s <http://www.georss.org/georss/point> ?pos. ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+type+">}"; break;
		//ISA
		case 11: query=""; break;
		//W3C Basic Geo Geometry
		case 16: query="SELECT (count(DISTINCT ?y) as ?c) WHERE { ?g <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?y. ?s <http://www.w3.org/2003/01/geo/wgs84_pos#geometry> ?g. ?s a <"+type+">}"; break;
		}	
		
		return query;
	}


} //End Class
