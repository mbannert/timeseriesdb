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
                    tabPanel("Build Query",
                             tags$h2("Step 1: Search by Key"),
                             uiOutput("search_type"),
                             tags$form(
                               textInput("key", "search for Key", "")
                               , br()
                               , actionButton("button1", "Search timeseriesdb")
                             ),
                             textOutput("hits")     
                    ),
                    tabPanel("Plot and Export",
                             plotOutput("plot"),
                             fluidRow(
                               column(2,radioButtons("legend", "Use legend?",
                                                     c("Yes" = "yes",
                                                       "No" = "no"))),
                               column(6,uiOutput("choices")),
                               column(4,tags$h2("Export"),
                                      radioButtons("wide", "Use wide format?",
                                                   c("Yes" = "yes",
                                                     "No" = "no")),
                                      downloadButton('download', 'Download')
                               )
                             )
                    ),
                    tabPanel("Time Series Sets",
                             tags$form(
                               textInput("set_name", "Give a set name", "")
                               , br()
                               , actionButton("button2", "Store the time series set")
                             ),
                             textOutput("store_set") 
                    ),
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
                          width:400px !important;
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
        
        radioButtons("search_type", "Search Type",
                     st_keys)
        
        
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
           storeTsSet(con, isolate(input$set_name), set_list, 'gbucur')
       
           otext <- paste('You have stored the set ', isolate(input$set_name), '!!!')
        }

        otext
      })
      
      
    },
    options=list(launch.browser = T)
  )
  
  
}





