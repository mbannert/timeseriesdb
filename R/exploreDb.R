#' Start a GUI to Explore Data
#' 
#' Start a graphical user interface in the user's standard web browser to search 
#' and explore time series data. Data can be searched using regular expressions for
#' keys. Hits can subsequently selected from a select box and are plotted in a joint
#' time series plot. 
#' 
#' @param con PostgreSQL Connection object
#' @export 
exploreDb <- function(con){
  library(shiny)
  
  if(!dbIsValid(con)) stop("Database connection is not valid. Can't start exploring data.")
  
  
  shinyApp(
    # UI PART FOR SHINY APP -----------------------------------------------
    ui = navbarPage("timeseriesdb Data Explorer",
                    tabPanel("Query Setup",
                             tags$h2("Create Query"),
                             selectInput("query_type","Select Query Type",
                                         c("Key Based Query" = "key",
                                           "Load Pre-Defined Set" = "set")),
                             uiOutput("search_type")
                    ),
                    tabPanel("Plot and Export",
                             fluidRow(
                               column(6,tags$h2("Variable Selection"),
                                      uiOutput("choices")),
                               column(4,tags$h2("Store As Set"),
                                      tags$form(
                                 textInput("set_name", "Name", "")
                                 , br()
                                 , actionButton("button2", "Store the time series set")
                               ),
                               textOutput("store_set") 
                                      ),
                               column(2,tags$h2("Export"),
                                      radioButtons("wide", "Use wide format?",
                                                   c("Yes" = "yes",
                                                     "No" = "no")),
                                      downloadButton('download', 'Download'))
                               
                             ),
                             fluidRow(
                               column(10,plotOutput("plot")),
                               column(2,uiOutput("legend_control"))
                             )
                    ),
                    
                    # currently not needed, but will be used to share and delete sets
                    # in future versions
#                     tabPanel("Time Series Sets",
#                              tags$h2("Load a Time Series Set"),
#                              fluidRow(
#                                tags$form(
#                                  textInput("set_name_load", "Give a set name", "")
#                                  , br()
#                                  , actionButton("button_load_ts_set", "Search timeseriesdb")
#                                )
#                              )      
#                     ),
                    header = 
                      tags$style(HTML("
                          @import url('//fonts.googleapis.com/css?family=Lato|Cabin:400,700');
                          
                          h2 {
                          font-family: 'Lato';
                          font-weight: 500;
                          line-height: 1.1;
                          color: #A2C3C9;
                          }
                          
                          select {
                          width:400px !important;
                          height:150px !important;
                          }
                          
                          input[type='text']{
                          width:300px !important;
                          }
                          
                          
                          
                          "))
    ),
    
    # SERVER PART FOR SHINY APP -----------------------------------------------    
    server = function(input, output) {
      library(timeseriesdb)
      
      keys <- reactive({
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
      })
      
      
      # dynamically created UI
      output$choices <- renderUI({
        
        input$button1
        selectInput('in5', paste0('Select keys (',
                                  length(isolate(keys())),' hits)'),
                    names(isolate(keys())),
                    multiple = T, selectize=FALSE)  
      })
      
      output$search_type <- renderUI({
        
          md_keys <- dbGetQuery(con,"SELECT DISTINCT k FROM 
                   (SELECT skeys(meta_data) as k 
                                FROM meta_data_unlocalized) as dt;")$k
          
          names(md_keys) <- md_keys
          st_keys <- c(c("Main ts_key" = "ts_key"),md_keys)
          
          sets <- listTsSets(con)
          
          
          html1 <- column(6,
                   radioButtons("search_type", paste(input$query_type),
                                st_keys),
                   tags$form(
                     textInput("key", "Search for Key", "")
                     , br()
                     , actionButton("button1", "Search timeseriesdb")
                   ),
                   textOutput("hits")
                   )    
         
          html2 <- column(6,
                          selectInput('ui_set_list',
                                      "Select a Set of Time Series",
                                      sets,
                                      multiple = T, selectize=FALSE),
                          actionButton("get_series", "Get time series from set")
                          )
          
                 
          switch(input$query_type, key = html1, set = html2)       
                 
      })
      
      output$hits <- renderText({
        input$button1
        paste0(length(isolate(keys()))," hits. Switch to the plot tab to select time series immediately from search results.")
      })
      
      
      output$plot <- renderPlot({
        # li <- readTimeSeries(input$key,con)
        if(is.null(input$in5)) return(NULL)
        
        li <- isolate(keys())
        li <- li[input$in5]
        class(li) <- append(class(li),"tslist")
        plot(li,use_legend = ifelse(input$legend == "yes",T,F),
             shiny_legend = T)    
        
      })
      
      # Download Handler ------------------------------------
      output$download <- downloadHandler(
        filename = function(){paste0("test.csv")}, #input$fname,
        content = function(file){
          # write.table(isolate(keys())[[1]],file)
          # don't forget to change separator
          exportTsList(isolate(keys())[input$in5],fname = file) 
        }
        
        
      )
      
      
      output$store_set <- renderText({
        
        input$button2
        
        otext <- ""
        
        set_list <- isolate({
          li <- as.list(rep(input$search_type, length(input$in5)))
          names(li) <- input$in5
          li
        })
        
        if(length(set_list) > 0 && isolate(input$set_name) != "") {
           storeTsSet(con, isolate(input$set_name), set_list)
       
           otext <- paste('You have stored the set ', isolate(input$set_name), '!!!')
        }

        otext
      })
      
      set_keys <- reactive({
        input$button_load_ts_set
        set <- loadTsSet(con, isolate(input$set_name_load))
        kvp <- unlist(strsplit(set$key_set[1], ","))
        lapply(kvp, )
      })
      
      
      output$load_set <- renderUI({
        
        input$button_load_ts_set
        
        set <- loadTsSet(con, isolate(input$set_name_load))
        
#         selectInput('in5', paste0('Select keys (',
#                                   length(isolate(keys())),' hits)'),
#                     names(isolate(keys())),
#                     multiple = T, selectize=FALSE)  
      })
      
      
      output$legend_control <- renderUI({
        if(is.null(input$in5)) return(NULL)
        column(2,radioButtons("legend", "Use legend?",
                              c("Yes" = "yes",
                                "No" = "no")))
        
        
      })
      
      
      
    },
    options=list(launch.browser = T)
  )
  
  
}





