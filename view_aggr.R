# Shiny app for viewing data in each file
library(shiny)
library(data.table)

app <- shinyApp(
  ui = fluidPage(
    
    # Add a title
    titlePanel("View Flux Data"),
    
    # Add a row for the main content
    fluidRow(
      
      # Create a space for the plot output
      plotOutput(
        "tsPlot", "100%", "500px"
      )
    ),
    
    # Create a row for additional information
    fluidRow(
      # Take up 2/3 of the width with this element  
      mainPanel(radioButtons()),
      
      # And the remaining 1/3 with this one
      sidebarPanel(actionButton("clear", "Clear Points"))
    )    
  )
)
