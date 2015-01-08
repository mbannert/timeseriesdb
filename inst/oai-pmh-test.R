library(XML)

test2 <- serveDublinCore('ts1',lang='en',description = 'test')



newXMLNode


x = newXMLNode("block", "xyz", attrs = c(id = "bob"),
               namespace = c("fo" = "http://www.fo.org"))

metadata <- newXMLNode('metadata',
                       namespace = c('dc' = 'http://purl.org/dc/elements/1.1/'))



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