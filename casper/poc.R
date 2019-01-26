library(shiny)
library(data.table)
library(lubridate)
library(magrittr)
library(shinyTime)
library(shinycssloaders)
library(DT)
library(shinyalert)
library(purrr)



ui <- fluidPage(
    useShinyalert(),
    selectInput("flat","Flat number",choices=101:199,selected = NA),
    selectInput("resourcename","RESOURCE",choices = c("Badminton Hall","Party Hall","Clubroom"),selected = "Party Hall",multiple = F),
    dateInput("date",label = "Date",min = now(),max = now()+ddays(30),value = now() + ddays(1),startview = "month"),
    numericInput("sttime",label = "Start Hour",value = hour(now())+1,step = 1,min = 6,max = 23),
    numericInput("endtime",label = "End Hour",value =hour(now()) + 2,step = 1,min = 6,max = 24),
    actionButton("submit","SUBMIT"),
    h3(textOutput("confirm"))
)

server <- function(input,output){
  data<- fread("data.csv")
  data[,int:=lubridate::interval(strt,end)]
  
  observeEvent(input$submit,{
    strttime <- paste(as.character(input$date),paste(input$sttime,":00")) %>% ymd_hm(tz = "Asia/Kolkata") 
    endtime <- paste(as.character(input$date),paste(input$endtime,":00")) %>% ymd_hm(tz = "Asia/Kolkata") 
    newint <- strttime %--% endtime
    if(endtime<strttime) {
      shinyalert(title = "Error in Date/Time",text="Check end time is greater than start time",type = "error")
    } else 
      if(seq_len(nrow(data)) %>% map(~int_overlaps(newint,data$int[.x])) %>% unlist %>% any)
      {
        shinyalert(title = "CLASH",text="Your booking time period clashes with atleast 1 more booking",type = "error")
      } else 
      {
    fwrite(data.table(flat=input$flat,resource=input$resourcename,strt=strttime,end=endtime,status="Booked"),append = T,file = "data.csv")
    output$confirm <- renderText({
      paste("Your resource", input$resourcename,"has been blocked from",strttime,"to", endtime)
    })
  }
})
}

shinyApp(ui,server)