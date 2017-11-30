library(RMySQL)
library(DBI)
library(XML)
library(magrittr)
library(stringr)
library(formattable)
library(tidyr)
library(dplyr)
library(lubridate)
library(jsonlite)
library(compareDF)

this.file <- parent.frame(2)$ofile
dev <- T
ubuntu <- F
if(ubuntu) 
    library(plumber)

if(dev) database_name="jupiter_dev" else database_name="jupiter"
function_source <- ifelse (ubuntu, ifelse(this.file=="~/usr/local/plumber/learniatApi/LApi.R", "~/usr/local/plumber/learniatApi/quick.R","~/usr/local/plumber/testApi/quick.R"), "~/Dropbox/R-wd/quick.R")
source(function_source)

#' @filter log
function(req){
  INDIA="Asia/Kolkata"
  cat(paste0(now(tz=INDIA), " : ", req$REMOTE_ADDR, " : ",req$REQUEST_METHOD, " : ", req$PATH_INFO),"\n")
  forward()
}

# #' @filter logger
# function(...){
#   print(paste0(...))
#   forward()
# }

# @filter log
# function(req){
#   cat("Incoming request for", req$PATH_INFO, "\n")
#   forward()
# }


#* @post /mirror
mirror<-function(session_details=NULL){
  list(session_details=session_details)
}

#* @get /GetAllStudentStates
GetAllStudentStates<-function(SessionId=0,userid=0){
  log("GetAllStudentStates",userid,paste0("SessionId:",SessionId))
  students(classid(SessionId))[,c("student_id","user_state")]
}

#* @get /ChangeSessionState
ChangeSessionState<-function(userid=0,SessionId=0,NewState=0){
  output_df<-data.frame(OldState=integer(1),NewState=integer(1),Status=character(1))
  INDIA="Asia/Kolkata"
  if(!valid_user(userid) || SessionId==0)
  {
    output_df$Status<-"ERROR: Valid user_id and SessionId mandatory inputs"
    return(output_df)
  } else
    script1<-paste("SELECT * FROM class_sessions WHERE class_session_id=",SessionId)
  
  sess_record<-querysql(script1)
  old_state<-sess_record$session_state
  starting_time<-sess_record$starts_on
  ending_time <- sess_record$ends_on
  
    if (NROW(old_state)==0)
  {
    output_df$Status<-"ERROR: This SessionId was not found"
    return(output_df)
  } else
  {
    output_df$SessionId=SessionId
    output_df$OldState=old_state
    if(!old_state %in% c(1:6)) output_df$Status<-"Warning: Invalid Session state found in DB"
  }
  if(NewState>0) {
    script2<-"UPDATE class_sessions SET session_state = ?state WHERE class_session_id=?id LIMIT 1"
    script2<-sqlInterpolate(ANSI(),script2,state=NewState,id=SessionId)
    n2=runsql(script2)
  } 
  if(NewState==5 && ending_time > now()){
      script4<-sprintf("UPDATE class_sessions SET ends_on = '%s' WHERE class_session_id=%d LIMIT 1",now(),SessionId)
      n4=runsql(script4)
  }
  
  if(exists("n2") && n2==1) {
    output_df$NewState<-NewState
    script3<-"INSERT INTO state_transitions (entity_type_id,  entity_id, from_state,  to_state, transition_time) VALUES (2, ?eid, ?f,?t,?tm)"
    script3<-sqlInterpolate(ANSI(),script3, eid=SessionId, f=old_state,t=NewState,tm=as.character(now(tzone = INDIA)))
    n3<-runsql(script3)
    output_df$Status="Success"
  } else output_df$Warning<-"There were no changes made in DB"
  
  if(exists("n4") && n4==1) {
      message("End Time of session has been adjusted upwards")
  }
  
  log(api="ChangeSessionState",
      user = userid,
      parameters = paste0("SessionId:",SessionId,",","NewState:",NewState),
      returned_value = toJSON(output_df))
  output_df
}

#* @get /UpdateMyState
#* @serializer unboxedJSON
UpdateMyState<-function(user=0,state=0){
  INDIA="Asia/Kolkata"
  if(!is.null(user) && !(is.null(state))){
      user_record <- user_status(user) # this will ensure we query the SQL table only once
      old_state<-user_record$user_state 
      if(state!=user_record$user_state) {
          user_record$user_state<-state
          script<-"UPDATE tbl_auth SET user_state = ?x where user_id=?y LIMIT 1"
          sql<-sqlInterpolate(ANSI(),script,x=state,y=user)
          rows_modified<-runsql(sql)
          if(rows_modified==0) message("State was not changed") else {
              script2<-sprintf("INSERT INTO state_transitions (entity_type_id, entity_id, from_state, to_state,transition_time) VALUES (%d, %d, %d, %d, '%s')",
                               1,as.numeric(user),as.numeric(old_state),as.numeric(state),now())
              rows_added<- runsql(script2)
          }
      } else {
          rows_modified<-0
          user_record$warning <-"No Update happened"
          message("State was not changed")
      }
        outp<- user_record      
  } else outp<- data.table(Error="Need two parameters to this API, user and state")
log(api="UpdateMyState",user=user, parameters= paste0("state:",state),returned_value= toJSON(outp))
outp
}


#* @get /GetSessionInfo
GetSessionInfo<-function(SessionId=0,userid=0){
  log("GetSessionInfo",userid, paste("SessionId:",SessionId))
  INDIA="Asia/Kolkata"
  if(is.null(SessionId)) return("error_message:SessionId cannot be NULL") else {
    script<-paste("SELECT * FROM class_sessions where class_session_id=",SessionId)
    querysql(script) %>%
      merge(table_all("tbl_auth"),by.x="teacher_id",by.y="user_id") %>% 
      select(StartTime=starts_on,EndTime=ends_on,SessionState=session_state,TeacherId=teacher_id,TeacherName=user_name) %>% 
      mutate(Status="Success") ->output_df
    output_df
  }
}

#* @get /GetSessionInfo2
GetSessionInfo2<-function(SessionId=0,userid=0){
  log("GetSessionInfo",userid, paste("SessionId:",SessionId))
  INDIA="Asia/Kolkata"
  if(is.null(SessionId)) return("error_message:SessionId cannot be NULL") else {
    script<-paste("SELECT * FROM class_sessions where class_session_id=",SessionId)
    querysql(script) %>%
      merge(table_all("tbl_auth"),by.x="teacher_id",by.y="user_id") %>% 
      select(StartTime=starts_on,EndTime=ends_on,SessionState=session_state,TeacherId=teacher_id,TeacherName=user_name) %>% 
      mutate(Status="Success") ->output_df
    output_df
    list(SessionInfo=output_df)
  }
}

topics<-function(class_id,tag=1){
  ifelse(tag<2,
         sql_text<-paste0("SELECT * FROM topic where topic_id in (select topic_id from lesson_plan where class_id=",class_id," and topic_tagged=",tag,")"),
         sql_text<-paste0("SELECT * FROM topic where topic_id in (select topic_id from lesson_plan where class_id=",class_id, ")")
  )
  
  t<-suppressWarnings(  querysql(sql_text,database))
  t2<-t
  suppressWarnings( merge(t,t2,by.x = "parent_topic_id",by.y = "topic_id")) ->t3
  t3[,!duplicated(colnames(t3))] %>% select(13,1,5,2,8:10) %>% rename(Main_topic=topic_name.y,Maintopic_id=parent_topic_id,SubTopic=topic_name.x,SubTopic_id=topic_id,GI_num=gi_num.x,GI_den=gi_den.x,PI=pi.x)
}

main_topics<-function(class_id=14){
  topics(class_id) %>% group_by(Main_topic) %>% tally %>% rename(Count_SubTopics=n) %>% print(n=Inf)
}

topics_inClass<-function(class_id=14,tag=1){
  ifelse(tag<2,
         sql_text<-paste0("SELECT * FROM topic where topic_id in (select topic_id from lesson_plan where class_id=",class_id," and topic_tagged=",tag,")"),
         sql_text<-paste0("SELECT * FROM topic where topic_id in (select topic_id from lesson_plan where class_id=",class_id, ")")
  )
  
  t<-suppressWarnings(  querysql(sql_text,database_name) )
  t2<-t
  suppressWarnings( merge(t,t2,by.x = "parent_topic_id",by.y = "topic_id")) ->t3
  t3[,!duplicated(colnames(t3))] %>% select(13,1,5,2,8:10) %>% rename(Main_topic=topic_name.y,Maintopic_id=parent_topic_id,SubTopic=topic_name.x,SubTopic_id=topic_id,GI_num=gi_num.x,GI_den=gi_den.x,PI=pi.x)
}

#* @get /Login
Login<-function(app_id=0, user_name="BLANK",pass=NULL,uuid="BLANK",lat=0,long=0,app_version=0)
{ 
  userid<-user(user_name)$user_id[1]
  
  #remove_ended()
  INDIA="Asia/Kolkata"
  if(!all(!is.null(app_id),is.character(user_name),is.character(pass))) return("Error in parameters: This API has 3 mandatory paramters: app_id, user_name and pass")
  input_df=data.frame(app_id=app_id,user_name=user_name,password=pass,UUID=uuid,lat=lat,long=long,app_version=app_version)
  request_time<-now()
  output_df=data.frame(Status=character(1),UserId=numeric(1), UserName=character(1), SchoolId=numeric(1),warning_message=character(1),last_updated=as.Date("2017-01-01", origin = "1900-01-01"),
                       error_message=character(1),existing_state=numeric(1),stringsAsFactors=F)
  output_df$UserName<-user_name
  db = dbConnect(MySQL(), user=db_user_name, password=db_password, dbname=database_name, host=host_address)
  on.exit(dbDisconnect(db))
  sql <- "SELECT * FROM tbl_auth WHERE user_name = ?name"
  sql_new<-sqlInterpolate(ANSI(), sql, name = user_name)
  user<-suppressWarnings(dbGetQuery(conn=db,statement=sql_new))
  if(nrow(user)==0){ output_df[1,"error_message"]<-dbGetQuery(conn=db,"select * from error_messages where error_code=105")[1,3];output_df[1,"Status"]<-"Error- CHANGED FOR TEST"; return(output_df)}
  password_is_correct=NULL
  if(!user$role_id==app_id)  {
    { output_df[1,"error_message"]<-dbGetQuery(conn=db,"select * from error_messages where error_code=104")[1,3];output_df[1,"Status"]<-"Error"; return(output_df)}
  } else password_is_correct=(user$password==pass)
  output_df$Status<- if(password_is_correct)  "Success" else "Error"
  output_df$UserId<-user$user_id
  output_df$SchoolId<-user$school_id
  if(password_is_correct) {
    connected_dev<-ifelse (is.na(user$connected_device_id),0,user$connected_device_id) 
    if (!input_df$UUID == connected_dev) {output_df$warning_message<-"This user is signed in from another device.. signing in from here also and changing state to 7"
                                          sql <- "UPDATE tbl_auth SET connected_device_id=?dev, last_updated=?dt WHERE user_id = ?id1;"
                                          sql_new<-sqlInterpolate(ANSI(),sql,dev=uuid,dt=as.character(now()),id1 = user$user_id)
                                          x<-dbExecute(db,sql_new)
    } 
    output_df$Status<-"Success"
    output_df$existing_state<-user$user_state
    output_df$last_updated<-user$last_updated
  } else output_df$error_message<-"Incorrect Password"

  if(output_df$Status=='Success'){
    #update sign in states in tbl_auth and state_transtions
    sql1 <- "UPDATE tbl_auth SET last_updated=now(), user_state=7 WHERE user_id = ?id1;"
    sql2 <- "INSERT INTO state_transitions (entity_type_id, entity_id, from_state, to_state,transition_time) VALUES (1, ?id2, 8,7,now())"
    sql_new1<-sqlInterpolate(ANSI(),sql1, id1 = user$user_id)
    sql_new2<-sqlInterpolate(ANSI(), sql2, id2 = user$user_id)
    x1<-dbExecute(db,sql_new1)
    x2<-dbExecute(db,sql_new2)
    if(any(x1>1,x2>1)) output_df$error_message<-"Something seriously wrong has happened, as more than one row was updated in tbl_auth"
  }

#   x<-update_event_log(db,service_name='Login', user_id=user$user_id,      UUID=uuid,         xml_input=paste(input_df[1,],collapse=";"),    request_time=request_time,
#                   xml_output=paste(output_df,collapse=";"),   return_time=now() )
#   if(!x==1) output_df$warning_message <-"Event log could not be updated or has updated many rows. Please check event_log table in jupiter_dev DB"
  log(api="Login",user=userid,parameters=paste(user_name,pass,uuid,app_id,lat,long,app_version),returned_value=toJSON(output_df))
  output_df
}

#* @get /Login2
Login2<-function(app_id=0, user_name="BLANK",pass=NULL,uuid="BLANK",lat=0,long=0,app_version=0)
{ 
  userid<-user(user_name)$user_id[1]
  log("Login",userid,paste(user_name,pass,uuid,app_id,lat,long,app_version))
  #remove_ended()
  INDIA="Asia/Kolkata"
  if(!all(!is.null(app_id),is.character(user_name),is.character(pass))) return("Error in parameters: This API has 3 mandatory paramters: app_id, user_name and pass")
  input_df=data.frame(app_id=app_id,user_name=user_name,password=pass,UUID=uuid,lat=lat,long=long,app_version=app_version)
  request_time<-now()
  output_df=data.frame(Status=character(1),UserId=numeric(1), UserName=character(1), SchoolId=numeric(1),warning_message=character(1),last_updated=as.Date("2017-01-01", origin = "1900-01-01"),
                       error_message=character(1),existing_state=numeric(1),stringsAsFactors=F)
  output_df$UserName<-user_name
  db = dbConnect(MySQL(), user=db_user_name, password=db_password, dbname=database_name, host=host_address)
  on.exit(dbDisconnect(db))
  sql <- "SELECT * FROM tbl_auth WHERE user_name = ?name"
  sql_new<-sqlInterpolate(ANSI(), sql, name = user_name)
  user<-suppressWarnings(dbGetQuery(conn=db,statement=sql_new))
  if(nrow(user)==0){ output_df[1,"error_message"]<-dbGetQuery(conn=db,"select * from error_messages where error_code=105")[1,3];output_df[1,"Status"]<-"Error- CHANGED FOR TEST"; return(output_df)}
  password_is_correct=NULL
  if(!user$role_id==app_id)  {
{ output_df[1,"error_message"]<-dbGetQuery(conn=db,"select * from error_messages where error_code=104")[1,3];output_df[1,"Status"]<-"Error"; return(output_df)}
  } else password_is_correct=(user$password==pass)
  output_df$Status<- if(password_is_correct)  "Success" else "Error"
  output_df$UserId<-user$user_id
  output_df$SchoolId<-user$school_id
  if(password_is_correct) {
    connected_dev<-ifelse (is.na(user$connected_device_id),0,user$connected_device_id) 
    if (!input_df$UUID == connected_dev) {output_df$warning_message<-"This user is signed in from another device.. signing in from here also and changing state to 7"
                                          sql <- "UPDATE tbl_auth SET connected_device_id=?dev, last_updated=?dt WHERE user_id = ?id1;"
                                          sql_new<-sqlInterpolate(ANSI(),sql,dev=uuid,dt=as.character(now()),id1 = user$user_id)
                                          x<-dbExecute(db,sql_new)
    } 
    output_df$Status<-"Success"
    output_df$existing_state<-user$user_state
    output_df$last_updated<-user$last_updated
  } else output_df$error_message<-"Incorrect Password"
  
  if(output_df$Status=='Success'){
    #update sign in states in tbl_auth and state_transtions
    sql1 <- "UPDATE tbl_auth SET last_updated=now(), user_state=7 WHERE user_id = ?id1;"
    sql2 <- "INSERT INTO state_transitions (entity_type_id, entity_id, from_state, to_state,transition_time) VALUES (1, ?id2, 8,7,now())"
    sql_new1<-sqlInterpolate(ANSI(),sql1, id1 = user$user_id)
    sql_new2<-sqlInterpolate(ANSI(), sql2, id2 = user$user_id)
    x1<-dbExecute(db,sql_new1)
    x2<-dbExecute(db,sql_new2)
    if(any(x1>1,x2>1)) output_df$error_message<-"Something seriously wrong has happened, as more than one row was updated in tbl_auth"
  }
  
  x<-update_event_log(db,service_name='Login', user_id=user$user_id,      UUID=uuid,         xml_input=paste(input_df[1,],collapse=";"),    request_time=request_time,
                      xml_output=paste(output_df,collapse=";"),   return_time=now() )
  if(!x==1) output_df$warning_message <-"Event log could not be updated or has updated many rows. Please check event_log table in jupiter_dev DB"
  output_df
  list(Value=output_df)
}

#* @get /GetMyTodaysSessions
#* @serializer unboxedJSON
GetMyTodaysSessions<-function(user=0,uuid="BLANK",refresh_minutes=10) {
  users<-list_users(refresh=F)
  if(length(intersect(user,users$user_id))==0) {
    outp<-list(Sessions=NA,Error="Incorrect userid")
    log(api="GetMyTodaysSessions",user=user,parameters="BLANK",returned_value= toJSON(outp))
    return(outp) 
  }
 refresh("d$student_class_map",time_gap_hours=refresh_minutes/60)->> d$student_class_map
 refresh("d$seat_assignments",time_gap_hours=refresh_minutes/60)->> d$seat_assignments
  stud_counts<-d$student_class_map %>%
    merge(users,by.x="student_id",by.y="user_id") %>% 
    select(class_id,student_id,user_state) %>% group_by(class_id,user_state) %>% summarise(stud_count=n())
  today_sessions<-show_sessions(u=user,days = 1,limit = 50)%>% filter(date(starts_on)==Sys.Date())
  valid_sessions<-today_sessions$class_session_id
  seat_summary<-d$seat_assignments %>% 
    group_by(class_session_id) %>% 
    summarise(seats_configured=n()) %>% 
    filter(class_session_id %in% valid_sessions)
  today_sessions <- left_join(today_sessions,seat_summary,by="class_session_id")
  today_sessions <- left_join(today_sessions,d$rooms[,c(1,3)],by="room_id")
  master<-merge(today_sessions,stud_counts,by="class_id")
  if(nrow(master)>0){
    master %<>% spread(key = user_state,stud_count,sep = ":") #create columns for states
    for (i in c(1,7,8,9,10,11)){
      if(length(intersect(names(master),var_col<-paste0("user_state:",i)))==0) master %<>% mutate(!!var_col := 0) # add a new state column if there was no column for the state as zero students were in the state
    }
    master%<>% left_join(classes(u=user),by="class_id")
    master %>% 
      mutate(PreAllocatedSeats=ifelse(session_state%in% c(4,2),`user_state:9`,0), # for session state 2 or 4, PreAllocated Seats = count of students in state=9
             OccupiedSeats=ifelse(session_state%in% c(1,2),`user_state:1`+`user_state:10`,0), # OccupiedSeats are nothing but count of students in state 1 and state 10, in a session in state 1 or 2.
             SessionGI=percent(gi_num/gi_den,digits = 1),
             SessionPI=pi.x,
             ClassGI=GI,
             ClassPI=pi.y,
             teacher_id=teacher_id.x) %>% 
      select(-stud_attended,-total_stud_registered,-teacher_id.y,-teacher_id.x,-GI) ->output
    outp<-list(Sessions=output)
  } else outp<- list(status="No class sessions for the day")
  log(api="GetMyTodaysSessions",user=user,parameters="BLANK",returned_value= toJSON(outp))
  outp
}


#* @get /RefreshMyApp
RefreshMyApp<-function(userid=0,uuid="BLANK"){
    d$tbl_auth <<- refresh("d$tbl_auth",time_gap_hours = 0.001)
  #remove_ended()
  INDIA="Asia/Kolkata"
  old_rooms<-data.frame()
  current_session<-F
  next_session<-F
  if(userid==0) return(error_message<-"Mandatory parameter UserId is missing") else
    # extract state from tbl_auth
    user_state<-d$tbl_auth %>% filter(user_id==userid) %>% select(user_state) %>% .[,1]
  MyUserState<-8
  if(NROW(user_state)==1) {
    MyUserState<-user_state
    suppressWarnings(show_sessions(user=userid)) %>%
      select(class_session_id,starts_on,ends_on,session_state) -> session_list
    time<-now()
    #tz(session_list$starts_on)<-"Asia/Kolkata"
    #tz(session_list$ends_on)<-"Asia/Kolkata"
    m1<-time>session_list$starts_on
    m2<-time>session_list$ends_on
    m3<-xor(m1,m2)
    #if (sum(m3)>1) warn<-c(warn,"We have at least one overlapping class session")
    if(any(m3)) session_list[m3,]->current_session2
    if(any(m2)) session_list[m2,] %>% select(class_session_id,session_state) ->old_rooms
    
    if(exists("current_session2")){
      current_session<-TRUE
      MyCurrentSessionDetails<-current_session2
    } else {
      current_session<-FALSE
      MyCurrentSessionDetails<-NA
    }
    
    gaps<-interval(time,session_list$starts_on)
    if(NROW(gaps)) {gaps2<-time_length(gaps); pos<-gaps2[gaps2>0]}
    if (current_session) MyCurrentSessionDetails<-current_session2 else MyCurrentSessionDetails<-NA
    if(exists("pos") && NROW(pos)>0) min_pos<-min(pos)
    if(exists("min_pos")) { 
      truth_table<-gaps2==min_pos
      MyNextSessionDetails<-session_list[truth_table,]
      next_session<-TRUE
    } else {next_session<-FALSE;  MyNextSessionDetails<-NA}
    
    NextSession_id<-NA; NextSession_state<-NA
    
    if(next_session) 
    {
      MyNextSessionDetails$class_session_id -> NextSession_id
      MyNextSessionDetails$session_state -> NextSession_state
    }
    
    CurrSession_id<-NA; CurrSession_state<-NA
    if(current_session) 
    {
      CurrSession_state<- MyCurrentSessionDetails$session_state
      CurrSession_id <-MyCurrentSessionDetails$class_session_id
    }
    
    MySessionStates<-session_list[,c(1,4)]
    
#     if((is.na(CurrSession_id) && next_session) || ((CurrSession_state>4) && (next_session))) {
#       CurrSession_state<-NextSession_state
#       CurrSession_id<-NextSession_id
#     }

    
    output<-tibble(MyState=MyUserState,CurrentSessionState=CurrSession_state,CurrentSessionId=CurrSession_id,NextClassSessionId=NextSession_id,NextClassSessionState=NextSession_state)
    y<-list(Summary=output,AllSessions=MySessionStates,DestroyRooms=old_rooms)
    log(api = "RefreshMyApp",user = userid,parameters = "iMac",returned_value = toJSON(y))
    y
  } else return(error_message<-"This user does not exist")
}

#* @get /RefreshJoinedStudents
RefreshJoinedStudents<-function(user=NULL,session=NULL){
  joined<-data.frame(number=0,registered=nrow(students(session=session)))
  x<-students(session=session) %>% count(user_state) %>% filter(user_state==10)
  if(nrow(x)>0) joined$number <-x$n
  log(api="RefreshJoinedStudents",user=user, parameters=paste("session:",session),returned_value=toJSON(joined))
  joined
}

#* @get /RefreshJoinedStudents_unboxed
RefreshJoinedStudents_unboxed<-function(user=NULL,session=NULL){
    joined<-data.frame(number=0,registered=nrow(students(session=session)))
    x<-students(session=session) %>% count(user_state) %>% filter(user_state==10)
    if(nrow(x)>0) joined$number <-x$n
    log(api="RefreshJoinedStudents",user=user, parameters=paste("session:",session),returned_value=toJSON(joined))
    list(joined)
}


#* @get /InsertScribbleFileName
InsertScribbleFileName<-function(filename=NULL,user_id=0,image_type_id=5,uuid="BLANK"){
  INDIA="Asia/Kolkata"
  if (is.null(filename)) return(error_message<-"filename is mandatory")
  script<-"insert into uploaded_images (image_type_id,image_path,uploaded_by,active,DateUploaded) VALUES (?x1, ?x2, ?x3, 1,?x4 )"
  sql<-sqlInterpolate(ANSI(),script,x1=image_type_id,x2=filename,x3=user_id,x4=as.character(now()))
  x<-runsql(sql)
  retrieve_script<-"select image_id from uploaded_images where image_path = ?path"
  sql<-sqlInterpolate(ANSI(),retrieve_script,path=filename)
  x<-querysql(sql)
  log(api="InsertScribbleFileName",user=user_id, parameters=paste0("filename:",filename,",","image_type:",image_type_id,",","uuid:",uuid),returned_value=toJSON(x))
  x
}

#* @get /GetLiveStatus
#* @serializer unboxedJSON
GetLiveStatus<-function(userid=NULL){ 
  if(is.null(userid)){ 
    message("ERROR: Did not receive any userid. Check userid spelling in API")
    outp<- "ERROR: Did not receive any userid. Check userid spelling in API"
  }
  all_live<-suppressWarnings(table_all("live_session_status"))
  if(nrow(all_live)>0){
    live_session_id<-suppressWarnings(show_sessions(user=userid)) %>% 
      filter(session_state==1) %>%  .[,1]
    all_ids<-all_live[,1]
    if (all(NROW(live_session_id>0),NROW(all_ids)>0)) {
      x<-intersect(live_session_id,all_ids)
      if (NROW(x)==1) outp<- (all_live %>% filter(session_id==x)) else if (NROW(x)>1) outp<- "More than one sessions are LIVE: RUN!"
    }
  }
    else outp<- "NO LIVE SESSION"
    log(api="GetLiveStatus",user=ifelse(is.null(userid),"MISSED",userid),parameters="NIL",returned_value=toJSON(outp) )
    outp
}

#* @get /MarkTopicCompleted
#* @serializer unboxedJSON
MarkTopicCompleted<-function(userid=NULL,topicid=NULL,sessionid=NULL,remove=NULL){
  tt<-c(is.null(topicid),is.null(sessionid))
  if(identical(tt,c(T,F))) return(list(Status='ERROR', error_message="topicid cannot be NULL"))
  if(identical(tt,c(F,T))) return(list(Status='ERROR', error_message="sessionid cannot be NULL"))
  if(identical(tt,c(T,T))) return(list(Status='ERROR', error_message="sessionid OR topicid cannot be NULL"))
  if(identical(tt,c(F,F))) script<-sprintf("select class_id from class_sessions where class_session_id = %s",sessionid)
  log("MarkTopicCompleted",userid, paste("TopicId:",topicid,"SessionId:",sessionid,"remove:",remove))
  classdf<-querysql(script)
  if (nrow(classdf)==1) {
    
    if (is.null(remove)) script1<-sprintf("update lesson_plan set topic_tagged=2, tagged_by=%s where class_id=%s AND topic_id=%s",userid,classdf$class_id,topicid ) else
      script1<-sprintf("update lesson_plan set topic_tagged=3, tagged_by=%s where class_id=%s AND topic_id=%s",userid,classdf$class_id,topicid )
    value1<-runsql(script1)
    
    if(value1>0) {
      script2<-sprintf("insert into topic_log (class_id, topic_id, class_session_id, cumulative_time, state) values (%s,%s,%s,%s,%s)",classdf$class_id,topicid,sessionid,19999,35)
      value2<-runsql(script2)
    } else return(list(Status='ERROR', error_message="update on lesson_plan table failed"))
    
    if(value2>0) return(list(Status='Success')) else return(list(Status='ERROR', error_message="insert in DB table topic_log failed"))
  } else return(list(Status='ERROR', error_message="sessionid is invalid "))
}

#* @post /RecordLessonTagging
#* @serializer unboxedJSON
RecordLessonTagging<-function(userid=NULL,classid=NULL,new_tags=data.table(topic_id=c(354,360:362),topic_tagged=c(0,0,0,1))){
  d$lesson_plan<-refresh("d$lesson_plan")
  old_tags<-d$lesson_plan[class_id==classid][topic_id %in% new_tags$topic_id,.(topic_id,topic_tagged)]
  changed_topics<-setdiff(new_tags,old_tags)
  change_number<-update_lessonplan(changed_topics,classid)
  if(change_number!=nrow(changed_topics))  {
    print(message("There was some problem hence all DB changes rolled back; here are the topic_tag changes inputted")) 
  }else
    log(api = "RecordLessonTagging",
        user = userid, 
        parameters = as.character(toJSON(data.frame(classid,new_tags),dataframe = 'rows')),
        returned_value = as.character(toJSON((changed_topics)))
    )
  changed_topics
}

#* @get /SetModel
#* @serializer unboxedJSON
SetModel<-function(userid=0,asses_id=0,model_flag=0){
    script<- sprintf("update assessment_answers set model_answer = %d where assessment_answer_id = %d",as.numeric(model_flag), as.numeric(asses_id))
    number<-runsql(script)
    if(number==1) status="SUCCESS" else
        status="No DB changes happened"
    log(api = "SetModel",
        user = userid, 
        parameters = as.character(toJSON(data.frame(asses_id,model_flag),dataframe = 'rows')),
        returned_value = as.character(toJSON((status)))
    )
    return(list(Status=status))
}


#* @get /list_classes
#* @serializer unboxedJSON
list_classes<-function(teacher=NULL,hours=1,userid=NULL){
    refresh("b$sessions100",time_gap_hours=hours) ->> b$sessions100
    refresh("d$classes",time_gap_hours=hours) ->> d$classes
    outp <- b$sessions100 %>% left_join(d$classes,by="class_id") %>% filter(teacher_id.x==teacher) %>% select(class_id,class_name) %>% distinct()
    log(api="list_classes",
        user=ifelse(is.null(userid),"MISSED",userid),
        parameters=paste0("teacher:",teacher),
        returned_value=toJSON(outp) )
    outp
}

#* @get /list_sessions
#* @serializer unboxedJSON
list_sessions<-function(teacher=NULL,userid=NULL,all_busy=F,only_vol=F,ans_quer=T,only_ans=F,hours=24){
  refresh("d$query_volunteer",time_gap_hours=hours)->>d$query_volunteer
  refresh("d$classes",time_gap_hours=hours)->>d$classes
  refresh("d$student_query",time_gap_hours=hours)->>d$student_query
  refresh("b$sessions100",time_gap_hours=hours)->>b$sessions100
  refresh("b$assesm",time_gap_hours=hours)->>b$assesm
 
  lvols<-d$query_volunteer[d$student_query,on=.(query_id),nomatch=0
                           ][,.(volunteers=.N,votes=sum(thumbs_up,thumbs_down,na.rm = T)),by=class_session_id]
  lquers<-d$student_query[,.(count_queries=.N),by=class_session_id]
  lans<-b$assesm[,.(count_ans=.N),by=class_session_id]
  if (all_busy)
    outp<- lans[lquers,on=.(class_session_id),nomatch=0
         ][lvols,on=.(class_session_id),nomatch=0
           ][b$sessions100,on="class_session_id",.(class_session_id,ends_on,class_id),nomatch=0
             ][d$classes,on="class_id",.(class_session_id,class_name,ends_on),nomatch=0
               ][order(-class_session_id)] else
                 if(only_vol) 
                   outp<- lvols[b$sessions100,on="class_session_id",.(class_session_id,ends_on,class_id),nomatch=0
                         ][d$classes,on="class_id",.(class_session_id,class_name,ends_on),nomatch=0
                           ][order(-class_session_id)] else
                             if(only_ans) 
                               outp<-lans[b$sessions100,on="class_session_id",.(class_session_id,ends_on,class_id),nomatch=0
                                    ][d$classes,on="class_id",.(class_session_id,class_name,ends_on),nomatch=0
                                      ][order(-class_session_id)] else
                                        if(ans_quer)
                                          outp<- lans[lquers,on="class_session_id",nomatch=0
                                               ][b$sessions100,on="class_session_id",.(class_session_id,ends_on,class_id),nomatch=0
                                                 ][d$classes,on="class_id",.(class_session_id,class_name,ends_on),nomatch=0
                                                   ][order(-class_session_id)] else
                                                   {
                                                     refresh("b$sessions100",time_gap_hours=hours) ->> sessions100
                                                     refresh("d$classes",time_gap_hours=hours) ->> d$classes
                                                     outp<- sessions100 %>% 
                                                       filter(teacher_id==teacher,session_state==5) %>% 
                                                       left_join(d$classes,by="class_id") %>% 
                                                       select(class_session_id,ends_on,class_name) %>%
                                                       outp<- arrange(desc(class_session_id)) %>% head(100)
                                                   }
  log(api="list_sessions",
      user=ifelse(is.null(userid),"Webapp",userid),
      parameters=paste0("teacher:",teacher,",","all_busy:",all_busy,",","only_vol",only_vol,",","only_ans:",only_ans,",","ans_quer:",ans_quer,",","hours:",hours),
      returned_value=toJSON(outp))
  outp
}


#* @get /session_det
#* @serializer unboxedJSON
session_det<-function(session_id=NULL,userid=NULL,hours=24){
refresh("d$rooms",time_gap_hours=hours) ->>d$rooms
outp<- b$sessions100 %>%
  filter(class_session_id==session_id) %>% 
  left_join(d$classes,by="class_id") %>% left_join(d$rooms,by="room_id") %>% 
  select(class_id,starts_on,ends_on,session_state,room_name,class_name)
log(api="session_det",
    user=ifelse(is.null(userid),"Webapp",userid),
    parameters=paste0("session_id:",session_id,",","hours:",hours),
    returned_value=toJSON(outp))
outp
}

#* @get /stud_list
#* @serializer unboxedJSON
stud_list<-function(session_id=NULL,userid=NULL,hours_index=1,hours_tblauth=10,hours_entitystates=100){
 
  refresh("b$sindx",time_gap_hours=hours_index) ->>b$sindx
  refresh("d$tbl_auth",time_gap_hours=hours_tblauth) ->>d$tbl_auth
  refresh("d$entity_states",time_gap_hours=hours_entitystates) ->>d$entity_states
  outp<- b$sindx %>% 
    filter(class_session_id==session_id) %>% 
    left_join(d$tbl_auth,by=c(student_id="user_id")) %>% 
    select(first_name,last_name,user_id=student_id,user_state) %>% 
    left_join(d$entity_states,by=c(user_state="state_id")) %>% 
    select(-user_state) %>% rename(user_state=state_description) %>%
    distinct()
  log(api="stud_list",
      user=ifelse(is.null(userid),"Webapp",userid),
      parameters=paste0("session_id:",session_id,",","hours_index:",hours_index,",","hours_tblauth",hours_tblauth,",","hours_entitystates:",hours_entitystates),
      returned_value=toJSON(outp))
  outp
}

#----these functions are non API endpoints and referred to my the API end point functions.

update_lessonplan<-function(update=NULL,classid=NULL){
  con=dbConnect(MySQL(),dbname=database_name,host=host_address)
  n<-0
  for(i in 1:NROW(update)){
    script<-sprintf("UPDATE lesson_plan SET topic_tagged=%s where topic_id = %s and class_id =%s LIMIT 1",update[i,3],update[i,1],classid)
    n<-dbExecute(con,script) +n
  }
  if(n!=NROW(update)) dbRollback(con) else dbCommit(con)
  dbDisconnect(con)
  return(n)
}

update_event_log<-function(db,service_name='UNKNOWN', user_id=0,      UUID=0,         xml_input='XYZ',    request_time=now(),
                           xml_output='Nothing',   return_time=now()){
  sql <- "INSERT INTO event_log values  (0,?service_name, ?user_id,  ?UUID, ?xml_input, ?request_time, ?xml_output, ?return_time, 'json'  )"
  sql_new<-sqlInterpolate(ANSI(), sql, service_name=service_name, user_id=user_id,      UUID=UUID,         xml_input=xml_input,    request_time=as.character(request_time), xml_output=xml_output,   return_time=as.character(return_time))
  x<-dbExecute(db,sql_new)
  x
}

classid_sql<-function(session_id){
  script<-paste("select class_id from class_sessions where class_session_id=",session_id)
  querysql(script) [,1]
}


#-----functions called from command line and NOT to be called from this source script----


restart<-function(api_file=this.file){
  message(paste("loading functions from", function_source))
  #pr <- plumb('/home/rstudio/usr/local/plumber/testApi/testApi.R')
  pr<- plumb(api_file)
  pr$run(port=8100)
}
r<-restart
