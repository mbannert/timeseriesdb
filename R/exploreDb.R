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
    ui = fluidPage(
      
      tags$form(
        textInput("key", "search for Key", "")
        , br()
        , actionButton("button1", "Search timeseriesdb")
      ),
      
      
      uiOutput("choices"),
      plotOutput("plot")
      
    ),
    
    # SERVER PART FOR SHINY APP -----------------------------------------------    
    server = function(input, output) {
      library(timeseriesdb)
      
      keys <- reactive({
        if(input$key != ""){
          keys <- con %k% input$key # double check this
          keys
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
      
      output$plot <- renderPlot({
        # li <- readTimeSeries(input$key,con)
        if(is.null(input$in5)) return(NULL)
        
        li <- isolate(keys())
        li <- li[input$in5]
        class(li) <- append(class(li),"tslist")
        plot(li)    
        
        
        
        
        
      })
      
      
    },
    options=list(launch.browser = T)
  )
  
  
}



