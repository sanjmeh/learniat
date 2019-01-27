library(shiny)
library(shinyWidgets)
library(data.table)
library(lubridate)
library(magrittr)
library(shinycssloaders)
library(shinyalert)
library(purrr)

add_booking <- function(data,booking){
  cat("\nBooking row passed to function\n")
  print(booking)
  #fdata <- data[resource==booking$resource]
  cat("\nFiltered rows of data\n")
  fdata <- data[resource==booking$resource]
  print(fdata)
  print(int_overlaps(fdata$strt %--% fdata$end,booking$strt %--% booking$end) %>% unlist)
  #print(seq_len(nrow(fdata)) %>% map(~int_overlaps(booking$int,fdata$int[.x])) %>%  unlist)
  #print(fdata)
  if(int_overlaps(fdata$strt %--% fdata$end,booking$strt %--% booking$end) %>% unlist %>% any)
    {
    shinyalert(title = "CLASH",text="Your booking time period clashes with atleast 1 more booking",type = "error")
    data
    } else
    {
      fwrite(booking[,status:="Booked"],append = T,file = "data.csv")
      shinyalert("BOOKING SUCCESS!",text = paste0("Your booking for '",booking$resource,"' is confirmed from\n",booking$strt," to ",booking$end),type = "success")
      rbind(data,booking)
    }
}

css <- 
  "
body {
background: #00ffff;
}

* { font-size:5vw;}

.green {
background: green;
}

.shiny-date-input {
font-size:3vw;
height:200%;
}

.control-label {
font-size:2vw;
color:6495ED;
}

.selectize-input {
font-size: 2vw;
line-height: 2;
}

 .selectize-dropdown-content {
font-size:2vw;
line-height: 2;
max-height: 50vh;
}

#submit {
font-weight: bold;
font-size: 4vw;
padding: 1vw 1vh;
}

.form-control {
font-size: 2vw;
line-height: 2;
height:200%
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
  data<- fread("data.csv")
  data$strt %<>% ymd_hms(tz = "Asia/Kolkata")
  data$end %<>% ymd_hms(tz = "Asia/Kolkata")
  #data[,int:=lubridate::interval(strt,end)]
  observeEvent(input$submit,{
    strttime <- paste(as.character(input$date),paste(input$sttime,":01")) %>% ymd_hm(tz = "Asia/Kolkata") 
    endtime <- paste(as.character(input$date),paste(as.numeric(input$endtime) - 1,":59")) %>% ymd_hm(tz = "Asia/Kolkata") 
    booking <- data.table(flat=input$flat,resource=input$resourcename,strt=strttime,end=endtime,status="UNCONFIRMED")
    if(endtime<strttime) {
      shinyalert(title = "Error in Date/Time",text="Check end time is greater than start time",type = "error")
    } else {
      data <<- add_booking(data,booking) 
      cat("\nFull data returned by function:\n")
      print(data)
    }
  })
  session$onSessionEnded(stopApp)
}

shinyApp(ui,server)