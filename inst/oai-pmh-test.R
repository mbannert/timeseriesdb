library(XML)

test2 <- serveDublinCore('ts1',lang='en',description = 'test')



newXMLNode

cat(metadata)

metadata <- newXMLNode('metadata',
                       namespace = c('dc' = 'http://purl.org/dc/elements/1.1/',
                                     'xsi'= 'http://www.w3.org/2001/XMLSchema-instance'))



listToXML <- function(node, sublist){
  for(i in 1:length(sublist)){
    child <- newXMLNode(names(sublist)[i], parent=node,namespace = 'dc');
    
    if (typeof(sublist[[i]]) == "list"){
      listToXML(child, sublist[[i]])
    }
    else{
      xmlValue(child) <- sublist[[i]]
    }
  } 
}

listToXML(metadata,test2)

doc <- newXMLDoc(metadata)



library(OAIHarvester)
# reading directly from an xml file is not a problem when served via 
# localhost for some reason it does not read my file... maybe the xsd
# check tomorrow.... 
baseurl <- 'http://localhost:1234/test2.xml'
baseurl <- "http://epub.wu.ac.at/cgi/oai2"
oaih_identify(baseurl)

m <- oaih_get_record(baseurl,identifier = NULL)$metadata
oaih_transform(m)
m <- oaih_transform(m[sapply(m, length) > 0L])

# this get records statement works...
# epub.wu.ac.at/cgi/oai2?verb=GetRecord&identifier=oai:epub.wu-wien.ac.at:epub-wu-01_8f4&metadataPrefix=oai_dc
