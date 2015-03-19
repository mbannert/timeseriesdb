#' Polymorphic Helpers for the Shiny Based GUI
#' 
#' Though R is not strictly OO it is sometimes useful to add some object orientation. 
#' In the case of a shiny app with an dynamically created user interface a bit
#' of pseudo polymorphism helps to avoid too many but,ifs and elses. 
#' Depending on the query type that is selected by the user a UI creation
#' method is called. 
#' 
#' @rdname createUI
createUI <- function(x,...) UseMethod("createUI")

#' @rdname createUI
createUI.key <- function(x,...){
  
  md_keys <- dbGetQuery(con,"SELECT DISTINCT k FROM 
                        (SELECT skeys(meta_data) as k 
                        FROM meta_data_unlocalized) as dt;")$k
  
  names(md_keys) <- md_keys
  st_keys <- c(c("Main ts_key" = "ts_key"),md_keys)
  
  column(6,
         radioButtons("search_type", "Key Based Query",
                      st_keys),
         tags$form(
           textInput("key", "Search for Key", "")
           , br()
           , actionButton("button1", "Search timeseriesdb")
         ),
         textOutput("hits")
  )    
}

#' @rdname createUI
createUI.set <- function(x,...){
  sets <- listTsSets(con)
  column(6,
         selectInput('button1',
                     "Select a Set of Time Series",
                     sets,
                     multiple = T, selectize=FALSE)
  )
  
  
}




#' Create Choices Boxes Dynamically
#' @rdname createChoices
createChoices <- function(x,input = NULL,keys = NULL, ...) UseMethod("createChoices")

#' @rdname createChoices
createChoices.key <- function(x,input = NULL,keys = NULL,...){
  
  input$button1
  if(length(keys) != 0){
    selectInput('in5', paste0('Select keys (',
                              length(isolate(keys)),' hits)'),
                names(isolate(keys)),
                multiple = T, selectize=FALSE)    
  } else {
    NULL
  }
}


#' @rdname createChoices
createChoices.set <- function(x,input = NULL,keys = NULL,...){
  input$get_series
  if(length(isolate(input$button1)) != 0){
    ts_keys <- loadTsSet(con,input$button1)$keys
    selectInput('in5', paste0("Time Series in set '",input$button1,"'"),
                ts_keys,
                multiple = T, selectize=FALSE)    
  } else {
    NULL
  }
}



#' Search Keys From Pre-Defined Or Key Based Queries
#' @rdname searchKeys
searchKeys <- function(x,input = NULL,...) UseMethod("searchKeys")

#' @rdname searchKeys
searchKeys.key <- function(x,input = NULL,...) {
  if(input$key != ""){
    if(input$search_type == "ts_key"){
      keys <- con %k% input$key # double check this
      keys  
    } else{
      "%m%" <- createMetaDataHandle(input$search_type)
      keys <- con %m% input$key
      keys
    }
  } else {
    NULL
  }
}

#' @rdname searchKeys
searchKeys.set <- function(x,input = NULL,....){
  # it's clear we're looking for a set named input$button1
  # for the current user, could improve this using tryCatch
  # instead of if
  if(!is.null(input$button1)){
    keys_chars <- loadTsSet(con,input$button1)$keys
    keys <- readTimeSeries(keys_chars,con)
    keys  
  } else {
    NULL
  }
  
}
