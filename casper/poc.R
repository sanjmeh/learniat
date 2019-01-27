library(shiny)
library(shinyWidgets)
library(data.table)
library(lubridate)
library(magrittr)
library(shinycssloaders)
library(shinyalert)
library(purrr)

add_booking <- function(booking){
  data<- fread("data.csv")
  data$strt %<>% ymd_hms(tz = "Asia/Kolkata")
  data$end %<>% ymd_hms(tz = "Asia/Kolkata")
  data$timestamp %<>% ymd_hms(tz = "Asia/Kolkata")
  
  cat("\nBooking row passed to function\n")
  print(booking)
  fdata <- data[resource==booking$resource]
  print(int_overlaps(fdata$strt %--% fdata$end,booking$strt %--% booking$end) %>% unlist)
  if(int_overlaps(fdata$strt %--% fdata$end,booking$strt %--% booking$end) %>% unlist %>% any) # we donot need map
    {
    shinyalert(title = "CLASH",text="Your booking time period clashes with atleast 1 more booking",type = "error")
    data
    } else
    {
      newrow <- booking[,status:="Booked"][,timestamp:=now()]
      cat("\nNew ROW to be added:\n")
      print(newrow)
      cat("\nColumns of data:\n")
      print(tibble::glimpse(data))
      fwrite(newrow,append = T,file = "data.csv")
      shinyalert("BOOKING SUCCESS!",text = paste0("Your booking for '",booking$resource,"' is confirmed from\n",booking$strt," to ",booking$end),type = "success")
      rbind(data,newrow)
    }
}

css <- 
  "
body {
background: #00ffff;
}

* { font-size:24px;}

.green {
background: green;
}

.shiny-date-input {
font-size:24px;
height:200%;
}

.shiny-input-container{
height:200%;
font-size:24px;
}

.control-label {
font-size:18px;
color:6495ED;
}

.form-control {
font-size: 36px;
line-height: 2;
height:200%
}

.selectize-input {
font-size: 18px;
line-height: 2;
}

 .selectize-dropdown-content {
font-size:18px;
line-height: 2;
max-height: 75vh;
}

#submit {
font-weight: bold;
font-size: 1.2em;
padding: 1vw 1vh;
}


"

ui <- fluidPage(
  useShinyalert(),
  tags$head(
    tags$style(css)
    ),
  fluidRow(
    column(width = 6,
           selectInput("flat","Flat number",choices=101:199,selected = NA),
           selectInput("resourcename","RESOURCE",choices = c("Badminton Court-1","Party Hall","Clubroom"),selected = "Party Hall",multiple = F)
    ),
    column(width = 6,
           dateInput("date",label = "Date",min = now(),max = now()+ddays(180),value = now() + ddays(1),startview = "month"),
           selectInput("sttime",label = "Start Hour",selected = 8,choices = 6:23,multiple = F,selectize = T),
           selectInput("endtime",label = "End Hour",selected = 18,choices = 7:24,multiple = F,selectize = T),
           actionButton(class = "btn-success","submit","SUBMIT")
    ),
    h3(textOutput("confirm"))
  )
)

server <- function(input,output,session){
  
  observeEvent(input$submit,{
    strttime <- paste(as.character(input$date),paste(input$sttime,":01")) %>% ymd_hm(tz = "Asia/Kolkata") 
    endtime <- paste(as.character(input$date),paste(as.numeric(input$endtime) - 1,":59")) %>% ymd_hm(tz = "Asia/Kolkata") 
    booking <- data.table(flat=input$flat,resource=input$resourcename,strt=strttime,end=endtime,status="UNCONFIRMED")
    if(endtime<strttime) {
      shinyalert(title = "Error in Date/Time",text="Check end time is greater than start time",type = "error")
    } else {
      data <<- add_booking(booking) 
    }
  })
  session$onSessionEnded(stopApp)
}

shinyApp(ui,server)