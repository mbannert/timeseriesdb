#' Serve Dublin Core Compliant Meta Information for a Single Record 
#' 
#' This function is experimental. It is meant to serve Dublin Core compliant meta information for single time series records. 
#' 
#' @param id character identifier, time series key. 
#' @param lookup_localized 
#' @export
serveDublinCore <- function(id,lang = 'de',
                            lookup_localized = "meta_data_localized",
                            lookup_unlocalized = "meta_data_unlocalized",
                            contibutor = NULL,
                            creator = NULL,
                            description = NULL,
                            format = NULL,
                            language = NULL,
                            publisher = NULL,
                            rights = NULL,
                            src = NULL,
                            subject = "",
                            title = NULL
                            ){
  
  if(exists(id,envir = get(lookup_localized))){
    mi <- get(id,envir = get(lookup_localized))[[lang]]
  }
  
  if(exists(id,envir = get(lookup_unlocalized))){
    fixed <- as.list(get(id,envir = get(lookup_unlocalized))[['fixed']])
    
    flex_nms <- get(id,envir = get(lookup_unlocalized))[['flexible']]$key
    flexible <- as.list(get(id,envir = get(lookup_unlocalized))
                          [['flexible']]$value)
    names(flexible) <- flex_nms
    
    full_mi <- c(fixed,flexible,mi)
  }
  
  
  
  
  dc_elements_list <- list(
    contributor = NULL,
    coverage  = paste(full_mi$md_coverage_temp,
                      full_mi$coverage_spatial,
                      sep = ' in '), 
    creator = ifelse(is.null(creator),
                     full_mi$md_generated_by,
                     creator),
    date = full_mi$md_resource_last_update, # generated on
    description = ifelse(is.null(description),
                         full_mi$description,
                         description),
    format = ifelse(is.null(format),'',format),
    identifier = id,
    language = lang,
    publisher = ifelse(is.null(publisher),'',publisher),
    relation = NULL,
    rights = ifelse(is.null(rights),'',rights),
    source = ifelse(is.null(src),full_mi$source,src),
    subject = ifelse(is.null(subject),full_mi$keywords,subject),
    title = ifelse(is.null(title),full_mi$title,title),
    type = NULL)
  
  dc_elements_list
  
  
}





