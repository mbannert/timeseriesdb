#' Start a GUI to Explore Data
#' 
#' Start a graphical user interface in the user's standard web browser to search 
#' and explore time series data. Data can be searched using regular expressions for
#' keys. Hits can subsequently selected from a select box and are plotted in a joint
#' time series plot. 
#' 
#' @param con PostgreSQL Connection object
#' @export 
exploreDb <- function(con, browser = F){
  library(shiny)
  
  if(!dbIsValid(con)) stop("Database connection is not valid. Can't start exploring data.")
  
  shinyApp(ui = navbarPage("timeseriesdb Data Explorer",
                           tabPanel("Build Query",
                                    fluidRow(
                                      selectInput("query_type","Select Query Type",
                                                  c("Key Based Query" = "key",
                                                    "Load Pre-Defined Set" = "set",
                                                    "Search Localized Meta Information" = "md")),
                                      uiOutput("query_builder")  
                                    )
                           ),
                           tabPanel("Plot and Export",
                                    fluidRow(
                                      column(6,tags$h2("Variable Selection"),
                                             uiOutput("choices")
                                      ),
                                      column(4,
                                             tags$h2("Store As Set"),
                                             tags$form(
                                               textInput("set_name", "Name", "")
                                               , br()
                                               , actionButton("button2", "Store the time series set"),
                                               textOutput("store_set") 
                                             )
                                             
                                      ),
                                      column(2,tags$h2("Export"),
                                             radioButtons("wide", "Use wide format?",
                                                          c("Yes" = "T",
                                                            "No" = "F")),
                                             downloadButton('download', 'Download'))
                                    ),
                                    fluidRow(
                                      column(10,plotOutput("plot")),
                                      column(2,uiOutput("legend_control"))
                                    )
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
                                           width:300px !important;
                                           }

                                           .row{
                                            margin-left:15px !important;
                                           }                                           
                                           
                                           
                                           "))
  ),
  server = function(input,output){
    # reactive stuff ----------------
    query_type <- reactive({
      out <- input$query_type
      if(out == "key"){
        class(out) <- append("key",class(out))
      } else if(out == "set") {
        class(out) <- append("set",class(out))
      } else {
        class(out) <- append(c("md","key"),class(out))
      }
      out
    })
    
    
    keys <- reactive({
      searchKeys(query_type(),input = input)
    })
    
    
    
    
    
    
    # outputs ----------------
    # flexible query builder 
    output$query_builder <- renderUI({
      createUI(query_type())        
    })
    
    # display the hits 
    output$hits <- renderText({
      input$button1
      paste0(length(isolate(keys())),
             " series found. Switch to the next tab to proceed.")
    })
    
    
    # flexible choices boxes
    output$choices <- renderUI({
      createChoices(query_type(),input = input,
                    keys = keys())
      
    })
    
    # store set 
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
        
        otext <- paste('You have stored the set ', isolate(input$set_name), '.')
      }
      
      otext
    })
    
    
    
    
    # download handler for export
    output$download <- downloadHandler(
      filename = function(){paste0("time_series_export_",
                                   gsub(" |:|-","_",Sys.time()),".csv")}, #input$fname,
      content = function(file){
        # write.table(isolate(keys())[[1]],file)
        # don't forget to change separator
        exportTsList(isolate(keys())[input$in5],fname = file,cast = input$wide) 
      }
      
      
    )
    
    # plot that reactive so changes in selection
    output$plot <- renderPlot({
      if(is.null(input$in5)) return(NULL)
      
      li <- isolate(keys())
      li <- li[input$in5]
      class(li) <- append(class(li),"tslist")
      plot(li,use_legend = ifelse(input$legend == "yes",T,F),
           shiny_legend = T)  
    })
    
    # switch legend on/off 
    # legends are not suitable when 
    # there are to many series selected
    output$legend_control <- renderUI({
      if(is.null(input$in5)) return(NULL)
      column(2,radioButtons("legend", "Use legend?",
                            c("Yes" = "yes",
                              "No" = "no")))
      
      
    })
  }, options=list(launch.browser = browser))
}
