library(magrittr)
library(tidyr)
library(RMySQL)
library(DBI)
library(stringr)
library(formattable)
library(XML)
library(dplyr)
library(lubridate)
library(tibble)
library(jsonlite)
library(png)
library(compareDF)
setwd("~/Dropbox/R-wd")
source("quick.R")

dev <- T
ubuntu <- F

if(dev) database_name="jupiter_dev" else database_name="jupiter"
if (ubuntu){
    source("~/usr/local/plumber/testApi/quick.R")
} else {
    source("~/Dropbox/R-wd/quick.R")
}


# load(dbaccessfile)
list_of_api<-function(){
    lines<-scan(file="api.R",what=character(),skip = 20)
    grepl("function",lines)
    i<-grepl("function",lines)
    fns_grouped<-{lines[i] %>% strsplit("<-")}
    listfns<-NULL
    for(i in 1: NROW(fns_grouped)){
        listfns<-c(listfns,fns_grouped[[i]][1])
    }
    sort(listfns)
}

#---- API local copies

GetAllStudentStates<-function(SessionId=NULL){
   # print(print_log())
  students(classid(SessionId))[,.(student_id,user_state)]
}

UpdateMyState<-function(user=NULL,state=NULL){
    if(!is.null(user) && !(is.null(state))){
        old_state<-user_status(user)$user_state
        new_state<-state
        script<-"UPDATE tbl_auth SET user_state = ?x where user_id=?y"
        sql<-sqlInterpolate(ANSI(),script,x=state,y=user)
        rows_modified<-runsql(sql)
        if(rows_modified==0) {
            user_record=user_status(user)
            user_record$warning<-"No Update happened"
            return(user_record)
        } else if (rows_modified>1)
            return(tibble(Error="Critical DB alert: Many rows updated - check DB logs")) else return(user_status(user))
    } else {

        return(tibble(Error="Need two parameters to this API, user and state"))
    }
}

ChangeSessionState<-function(userid=0,SessionId=0,NewState=NA){
   log("ChangeSessionState",userid, paste("SessionId:",SessionId,"NewState:",NewState))
  output_df<-data.frame(OldState=integer(1),NewState=integer(1),Status=character(1))
  INDIA="Asia/Kolkata"
  if(!valid_user(userid) || SessionId==0)
    {
    output_df$Status<-"ERROR: Valid user_id and SessionId mandatory inputs"
    return(output_df)
  } else
    script1<-paste("SELECT session_state FROM class_sessions WHERE class_session_id=",SessionId)
  old_state<-querysql(script1)[,1]
  if (NROW(old_state)==0)
  {
    output_df$Status<-"ERROR: This SessionId was not found"
    return(output_df)
    } else
  {
    output_df$SessionId=SessionId
    output_df$OldState=old_state
    if(!old_state %in% c(1:6)) output_df$Status<-"Warning: Invalid Sessionid found in DB"
  }
  if(is.numeric(NewState)) {
    script2<-"UPDATE class_sessions SET session_state = ?state WHERE class_session_id=?id LIMIT 1"
    script2<-sqlInterpolate(ANSI(),script2,state=NewState,id=SessionId)
    n2=runsql(script2)
  } else
    {
      output_df$Status<-"Error: Invalid Sessionid passed. SessionId has to be numeric"
      return(output_df)
    }

  if(!is.null(n2) && n2==1) {
    output_df$NewState<-NewState
    script3<-"INSERT INTO state_transitions (entity_type_id,  entity_id, from_state,  to_state, transition_time) VALUES (2, ?eid, ?f,?t,?tm)"
    script3<-sqlInterpolate(ANSI(),script3, eid=SessionId, f=old_state,t=NewState,tm=as.character(now(tzone = INDIA)))
    n3<-runsql(script3)
    output_df$Status="Success"
  } else output_df$Warning<-"There were no changes made in DB"
    output_df
}


GetSessionInfo<-function(SessionId=NULL,userid=NULL,uuid=NULL){
    request_time=now()
    if(is.null(SessionId)) return("error_message:SessionId cannot be NULL") else {
        script<-paste("SELECT * FROM class_sessions where class_session_id=",SessionId)
        querysql(script) %>%
         merge(table_all("tbl_auth"),by.x="teacher_id",by.y="user_id") %>%
         select(StartTime=starts_on,EndTime=ends_on,SessionState=session_state,TeacherId=teacher_id,TeacherName=user_name) %>%
         mutate(Status="Success") ->output_df
        # x<-update_event_log(db,service_name='GetSessionInfo', user_id=userid, UUID=uuid, xml_input=list(SessionId=SessionId,userid=userid,uuid=uuid,request_time=request_time,
        #                                                                                                              xml_output=paste(output_df,collapse=";"),
        #                                                                                             return_time=now())
        #                     )
        #                     if(!x==1) output_df$warning_message <-"Event log could not be updated or has updated too many rows. Please check event_log table"
     output_df
    }
}



Login<-function(app_id=stop("app_id is mandatory"), user_name=stop("Usermame is mandatory"),pass=stop("password is mandatory"),uuid="NOT PASSED",lat=0,long=0,app_version=0)
{
    log("Login",userid,paste(user_name,pass,uuid,app_id,lat,long,app_version))
    input_df=data.frame(app_id=app_id,user_name=user_name,password=pass,UUID=uuid,lat=lat,long=long,app_version=app_version)
    request_time<-now()
    output_df=data.frame(Status=character(1),UserId=numeric(1), SchoolId=numeric(1),warning_message=character(1),last_updated=as.Date("2017-01-01", origin = "1900-01-01"),error_message=character(1),existing_state=numeric(1),stringsAsFactors=F)
    db = dbConnect(MySQL(), user=db_user_name, password=db_password, dbname=database_name, host=host_address)
    on.exit(dbDisconnect(db))
    sql <- "SELECT * FROM tbl_auth WHERE user_name = ?name"
    sql_new<-sqlInterpolate(ANSI(), sql, name = user_name)
    user<-suppressWarnings(dbGetQuery(conn=db,statement=sql_new))
    if(nrow(user)==0){ output_df[1,"error_message"]<-dbGetQuery(conn=db,"select * from error_messages where error_code=105")[1,3];output_df[1,"Status"]<-"Error"; return(output_df)}
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
        #update sign-in states in tbl_auth and state_transtions
        sql1 <- "UPDATE tbl_auth SET last_updated=now(), user_state=7 WHERE user_id = ?id1;"
        sql2 <- "INSERT INTO state_transitions VALUES (1, ?id2, 8,7,now())"
        sql_new1<-sqlInterpolate(ANSI(),sql1, id1 = user$user_id)
        sql_new2<-sqlInterpolate(ANSI(), sql2, id2 = user$user_id)
        x1<-dbExecute(db,sql_new1)
        x2<-dbExecute(db,sql_new2)
        if(any(x1>1,x2>1)) output_df$error_message<-"Something seriously wrong has happened, as more than one row was updated in tbl_auth"
    }

    x<-update_event_log(db,service_name='login', user_id=user$user_id,      UUID=uuid,         xml_input=paste(input_df[1,],collapse=";"),    request_time=request_time,
                        xml_output=paste(output_df,collapse=";"),   return_time=now() )
    if(!x==1) output_df$warning_message <-"Event log could not be updated or has updated many rows. Please check event_log table in jupiter DB"
    output_df
}

#* @get /GetMyTodaysSessions2
#* @serializer unboxedJSON
GetMyTodaysSessions2<-function(user=NULL,uuid=NULL)
{
    log("GetMyTodaysSessions",user,"iMac")
    users<-list_users()
    if(length(intersect(user,users$user_id))==0) return(list(Sessions=NA,Error="Incorrect userid passed"))
    stud_counts<-ta("student_class_map") %>%
        merge(users,by.x="student_id",by.y="user_id") %>% 
        select(class_id,student_id,user_state) %>% group_by(class_id,user_state) %>% summarise(stud_count=n())
    today_sessions<-show_sessions(u=user,days = 1,limit = 50)
    valid_sessions<-today_sessions$class_session_id
    seat_summary<-ta("seat_assignments") %>% group_by(class_session_id) %>% summarise(seats_configured=n()) %>% filter(class_session_id %in% valid_sessions)
    today_sessions <- left_join(today_sessions,seat_summary,by="class_session_id")
    today_sessions <- left_join(today_sessions,ta("rooms")[,c(1,3)],by="room_id")
    master<-merge(today_sessions,stud_counts,by="class_id")
    if(nrow(master)>0){
        master %<>% spread(key = user_state,stud_count,sep = ":")
        for (i in c(1,7,8,9,10,11)){
            if(length(intersect(names(master),var_col<-paste0("user_state:",i)))==0) master %<>% mutate(!!var_col := 0)
        }
        master%<>% left_join(classes(u=user),by="class_id")
        master %>% 
            mutate(PreAllocatedSeats=ifelse(session_state%in% c(4,2),`user_state:9`,0), OccupiedSeats=ifelse(session_state%in% c(1,2),`user_state:1`+`user_state:10`,0),
                   SessionGI=percent(gi_num.x/gi_den.x,digits = 1),
                   SessionPI=pi.x,ClassGI=percent(gi_num.y/gi_den.y,digits=1),ClassPI=pi.y,teacher_id=teacher_id.x) %>% 
            select(-stud_attended,-total_stud_registered,-teacher_id.y,-teacher_id.x,-GI) ->output
        list(Sessions=output)
    } else list(status="No class sessions for the day")
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
    t4<-proc.time()
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



session_list<-NULL

RefreshMyApp<-function(userid=0,uuid="BLANK"){
   
    INDIA="Asia/Kolkata"
    old_rooms<-data.frame()
    current_session<-F
    next_session<-F
    if(userid==0) return(error_message<-"Mandatory parameter UserId is missing") else
        # extract state from tbl_auth
        user_state<-table_all("tbl_auth") %>% filter(user_id==userid) %>% select(user_state) %>% .[,1]
   
    MyUserState<-8
    if(NROW(user_state)==1) {
        MyUserState<-user_state
        suppressWarnings(show_sessions(user=userid)) %>%
            select(1,5:6,7) -> session_list
        time<-now(tzone=INDIA)
        tz(session_list$starts_on)<-"Asia/Kolkata"
        tz(session_list$ends_on)<-"Asia/Kolkata"
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
        
        output<-tibble(MyState=MyUserState,CurrentSessionState=CurrSession_state,CurrentSessionId=CurrSession_id,NextClassSessionId=NextSession_id,NextClassSessionState=NextSession_state)
        y<-list(Summary=output,AllSessions=MySessionStates,DestroyRooms=old_rooms)
        log(api = "RefreshMyApp",user = userid,parameters = "iMac",returned_value = toJSON(y))
        y
    } else return(error_message<-"This user does not exist")
}



InsertScribbleFileName<-function(filename=NULL,user_id=NULL,image_type_id=5,uuid=NULL){
    if (any(is.null(filename),is.null(user_id))) return(error_message<-"filename and userid are mandatory")
    log("InsertScribbleFileName",user_id, paste("filename:",filename,"image_type_id:",image_type_id,"uuid:","iMac"))
    script<-"insert into uploaded_images (image_type_id,image_path,uploaded_by,active,DateUploaded) VALUES (?x1, ?x2, ?x3, 1,?x4 )"
    sql<-sqlInterpolate(ANSI(),script,x1=image_type_id,x2=filename,x3=user_id,x4=as.character(now()))
    x<-runsql(sql)
    retrieve_script<-"select image_id from uploaded_images where image_path = ?path"
    sql<-sqlInterpolate(ANSI(),retrieve_script,path=filename)
    x<-querysql(sql)
    toJSON(x)
}



GetLiveStatus<-function(userid=NULL){
    if(is.null(userid)) return("Error: 'userid' parameter is missed")
    all_live<-suppressWarnings(table_all("live_session_status"))
    live_session_id<-suppressWarnings(show_sessions(user=userid)) %>% filter(session_state==1) %>%  .[,1]
    all_ids<-all_live[,1]
    if (all(NROW(live_session_id>0),NROW(all_ids)>0)) {
        x<-intersect(live_session_id,all_ids)
        if (NROW(x)==1) return(all_live %>% filter(session_id==x)) else if (NROW(x)>1) return("More than one sessions are LIVE: RUN!") else return("NO LIVE SESSION")
    } else return("NO LIVE SESSION")

}

RefreshJoinedStudents<-function(user=NULL,session=NULL){
    joined<-data.frame(number=0,registered=nrow(students(session=session)))
        log("RefreshJoinedStudents",user, paste("session:",session))
        x<-students(session=session) %>% count(user_state) %>% filter(user_state==10)
        if(nrow(x)>0) joined$number <-x$n
        joined
    }



ClassSessionSummary<-function(SessionId=NULL){
    OccupiedSeats
    students(session = 4350) %>% count(user_state)
        
        ta("assign") %>% filter(class_session_id==SessionId) 

    
    
    # api=<Sunstone><Action><Service>ClassSessionSummary</Service><SessionId>4342</SessionId></Action></Sunstone>
    # {
    #     OccupiedSeats = 0;
    #     PreAllocatedSeats = 0;
    #     QuestionsConfigured = 51;
    #     SeatsConfigured = 14;
    #     SessionState = 4;
    #     StartTime = "2017-07-27 13:36:05";
    #     Status = Success;
    #     StudentsRegistered = 11;
    #     TopicsConfigured = 21;
    #     TotalAnswersReceived =     {
    #     };
    #     TotalMainTopicsConfigured =     {
    #     };
    #     TotalMaintopicsCompleted =     {
    #     };
    #     TotalQuestionsCompleted =     {
    #     };
    #     TotalSubtopicsCompleted = 15;
    #     TotalSubtopicsConfigured = 28;
    # }
    # 
    
}

GetMyTodaysSessionIDs<-function(UserId=496){
    r<-role(UserId)
    if(length(r)==1 && r==4){
        sql1<-"SELECT DISTINCT(class_id) FROM student_class_map WHERE student_id =?id"
        sql2<-paste("SELECT class_session_id FROM class_sessions WHERE date(starts_on) = CURDATE() AND class_id IN (", sql1,")")
        sql_new<-sqlInterpolate(ANSI(), sql2, id = UserId)
        sessions<-querysql(sql_new)
    } else  if(r==3){
        sql<-paste("SELECT class_session_id FROM class_sessions WHERE date(starts_on) = CURDATE() AND  teacher_id=",UserId)
        sessions<-querysql(sql)
    }
    sessions
}

MarkTopicCompleted<-function(userid=NULL,topicid=NULL,sessionid=NULL,remove=NULL){
    tt<-c(is.null(topicid),is.null(sessionid))
    if(identical(tt,c(T,F))) return(list(status='ERROR', error_message="topicid cannot be NULL"))
    if(identical(tt,c(F,T))) return(list(status='ERROR', error_message="sessionid cannot be NULL"))
    if(identical(tt,c(T,T))) return(list(status='ERROR', error_message="sessionid OR topicid cannot be NULL"))
    if(identical(tt,c(F,F))) script<-sprintf("select class_id from class_sessions where class_session_id = %s",sessionid)
    classdf<-querysql(script)
    log("MarkTopicCompleted",userid, paste("TopicId:",topicid,"SessionId:",sessionid,"remove:",remove))
    if (nrow(classdf)==1) {
        
        if (is.null(remove)) script1<-sprintf("update lesson_plan set topic_tagged=2, tagged_by=%s where class_id=%s AND topic_id=%s",userid,classdf$class_id,topicid ) else
            script1<-sprintf("update lesson_plan set topic_tagged=3, tagged_by=%s where class_id=%s AND topic_id=%s",userid,classdf$class_id,topicid )
        value1<-runsql(script1)
        
        if(value1>0) {
            script2<-sprintf("insert into topic_log (class_id, topic_id, class_session_id, cumulative_time, state) values (%s,%s,%s,%s,%s)",classdf$class_id,topicid,sessionid,19999,35)
            value2<-runsql(script2)
        } else return(list(status='ERROR', error_message="update on lesson_plan table failed"))
        
        if(value2>0) return(list(status='SUCCESS')) else return(list(status='ERROR', error_message="insert in DB table topic_log failed"))
    } else return(list(status='ERROR', error_message="sessionid is invalid "))
}


RecordLessonTagging<-function(userid=NULL,classid=NULL,new_tags=data.table(topic_id=c(354,360:362),topic_tagged=c(0,0,0,1))){
    d$lesson_plan<-refresh("d$lesson_plan")
    #old_tags<-d$lesson_plan %>% filter(class_id==classid) %>% filter(topic_id %in% new_tags$topic_id) %>% select(topic_id,topic_tagged)
    old_tags<-d$lesson_plan[class_id==classid][topic_id %in% new_tags$topic_id,.(topic_id,topic_tagged)]
    #ctags<-compare_df(old_tags,new_tags,group_col = "topic_id")    
    #changed_topics<-ctags$comparison_df %>% filter(chng_type=="-")
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

update_lessonplan<-function(update=NULL,classid=NULL){
    con=dbConnect(MySQL(),user=db_user_name, password=db_password, dbname=database_name,host=host_address,port=port)
    number_rows<-0
    for(i in 1:nrow(update)){
        script<-sprintf("UPDATE lesson_plan SET topic_tagged=%s where topic_id = %s and class_id =%s LIMIT 1",update$topic_tagged[i],update$topic_id[i],classid)
        number_rows<-dbExecute(con,script) + number_rows
    }
    if(number_rows!=nrow(update)) dbRollback(con) else dbCommit(con)
    dbDisconnect(con)
    return(number_rows)
}

#-----functions called from Learniat webapp
list_classes<-function(teacher=NULL,DB=F){
  if(DB){
    refresh("b$sessions100",time_gap_hours=0) ->> b$sessions100
    refresh("dclasses",time_gap_hours=0) ->> d$classes
  } else
  {
    refresh("b$sessions100",time_gap_hours=1) ->> b$sessions100
    refresh("d$classes",time_gap_hours=1) ->> d$classes
  }
  b$sessions100 %>% left_join(d$classes,by="class_id") %>% filter(teacher_id.x==teacher) %>% select(class_id,class_name) %>% distinct()
}

list_sessions<-function(teacher=NULL,all_busy=F,only_vol=F,ans_quer=F,only_ans=F){
    lvols<-d$query_volunteer[d$student_query,on=.(query_id),nomatch=0][,.(volunteers=.N,votes=sum(thumbs_up,thumbs_down,na.rm = T)),by=class_session_id]
    lquers<-d$student_query[,.(count_queries=.N),by=class_session_id]
    lans<-b$assesm[,.(count_ans=.N),by=class_session_id]
    if (all_busy)
        lans[lquers,on=.(class_session_id),nomatch=0
             ][lvols,on=.(class_session_id),nomatch=0
               ][b$sessions100,on="class_session_id",.(class_session_id,ends_on,class_id),nomatch=0
                 ][d$classes,on="class_id",.(class_session_id,class_name,ends_on),nomatch=0
                   ][order(-class_session_id)] else
                     if(only_vol) 
                         lvols[b$sessions100,on="class_session_id",.(class_session_id,ends_on,class_id),nomatch=0
                                  ][d$classes,on="class_id",.(class_session_id,class_name,ends_on),nomatch=0
                                    ][order(-class_session_id)] else
                                      if(only_ans) 
                                          lans[b$sessions100,on="class_session_id",.(class_session_id,ends_on,class_id),nomatch=0
                                               ][d$classes,on="class_id",.(class_session_id,class_name,ends_on),nomatch=0
                                                 ][order(-class_session_id)] else
                                                     if(ans_quer)
                                                         lans[lquers,on="class_session_id",nomatch=0
                                                              ][b$sessions100,on="class_session_id",.(class_session_id,ends_on,class_id),nomatch=0
                                                                ][d$classes,on="class_id",.(class_session_id,class_name,ends_on),nomatch=0
                                                                  ][order(-class_session_id)] else
                                               {
                                                   refresh("b$sessions100",time_gap_hours=3) ->> b$sessions100
                                                   refresh("d$classes",time_gap_hours=3) ->> d$classes
                                                   b$sessions100 %>% 
                                                       filter(teacher_id==499,session_state==5) %>% 
                                                       left_join(d$classes,by="class_id") %>% 
                                                       select(class_session_id,ends_on,class_name) %>% 
                                                       arrange(desc(class_session_id)) %>% head(100)
                                               }
}

#* @get /session_det
#* @serializer unboxedJSON
session_det<-function(session_id=NULL){
    refresh("d$rooms",time_gap_hours=24) ->>drooms
    b$sessions100 %>%
        filter(class_session_id==session_id) %>% 
        left_join(d$classes,by="class_id") %>% left_join(d$rooms,by="room_id") %>% 
        select(class_id,starts_on,ends_on,session_state,room_name,class_name)
}


#* @get /stud_list
#* @serializer unboxedJSON
stud_list<-function(session_id=NULL){
    refresh("b$st_index",time_gap_hours=1)->st_index
    refresh("d$tbl_auth",time_gap_hours=24)->d$tbl_auth
    refresh("d$entity_states",time_gap_hours=100)->d$entity_states
    st_index %>% 
        filter(class_session_id==session_id) %>% 
        left_join(d$tbl_auth,by=c(student_id="user_id")) %>% 
        select(first_name,last_name,user_id=student_id,user_state) %>% 
        left_join(d$entity_states,by=c(user_state="state_id")) %>% 
        select(-user_state) %>% rename(user_state=state_description) %>%
        distinct()
    
}

restart<-function() print("You are in the wrong app")
