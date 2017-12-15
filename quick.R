library(magrittr)
library(RMySQL)
library(DBI)
library(stringr)
library(formattable)
library(dplyr)
library(lubridate)
library(tibble)
library(png)
library(RCurl)
library(data.table)

#----initializations-------
loaded_file <- parent.frame(2)$ofile
INDIA="Asia/Kolkata"
dev <- T
if(str_detect(loaded_file,"Dropbox")) ubuntu<-F else ununtu <-T
MAMP <-F
 if(dev) database_name="jupiter_dev" else database_name="jupiter"
    dbname<-database_name
 
 if (ubuntu){
     host_address<-'localhost'
    # db_user_name <-'ahmed2'
    # db_password<-'ahmed123'
     ROOT<- "/var/www/logs"
     JSON_LOG<-"api.logs.txt"
     MYSQL_LOG <- "mysql.log"
     url_images <-  "http://54.251.104.13/images"
 } else {
     setwd("~/Dropbox/R-wd")
     dbaccessfile<- ifelse( MAMP,"localdbaccess", "remotedbaccess")
     JSON_LOG  <-"api.logs.txt"
     ROOT <- ifelse( MAMP,"/Applications/MAMP/db/mysql57", "http://54.251.104.13/logs")
     WROOT <- "/Applications/MAMP/db/mysql57"
     MYSQL_LOG <- ifelse( MAMP, "Kids-BR-iMac.log", "mysql.log")
     url_images <-  "http://54.251.104.13/images"
 }
    
    marker_b<-NULL
    marker_d<-NULL
    monitored_users<-496:505
    load(dbaccessfile)
#---main functions------
    
querysql<-function(sql_text=NULL,database=database_name){
    db<-DBI::dbConnect(RMySQL::MySQL(),user=db_user_name,password=db_password,dbname=database,
                       host=host_address,port=port)  # connect the DB
    on.exit(dbDisconnect(db))
    as.data.table(dbGetQuery(db,sql_text))
}

show_tbl_status<-function(dbname=database_name){
    querysql("show table status",database = dbname) %>% select("Name", "Rows", "Avg_row_length", "Data_length", "Index_length", "Auto_increment","Create_time") %>% 
        mutate(created=ymd_hms(Create_time)) %>% 
        select (-Create_time)
}

update_time <- function(df){
    attr(df,"last_upd") <- now()
    return(as.data.table(df))
}

reloadDB<- function(which=NULL){ 
    if(which=="big"){
        message(paste("ALERT: reloading big15 tables from:",ifelse(MAMP,"LOCAL MAC DB","AWS DB")))
        b <<- lapply(X=paste0("b$",short_names),FUN=refresh,time_gap_hours=0)
        names(b)<<- short_names
        marker_b <<- b$elog[1]$text
        val<-countbig()
    } else if(which=="small") {
        message(paste("ALERT: reloading small tables from:",ifelse(MAMP,"LOCAL MAC DB","AWS DB")))
        load_dbtables(); 
        marker_d <<- d$tbl_auth$latitude[1]
        val<-countsmall()
    }
    val
}


refresh<-function(variable_name=NULL,time_gap_hours=24,history=100,transition_history=30){
    if(grepl("$",variable_name,fixed = T)){
        expr<-paste("attr(",variable_name,",which='last_upd'",")")
        condition<- is.null(eval(parse(text = variable_name))) || is.null(eval(parse(text=expr))) || time_length(now()-eval(parse(text=expr)),unit="hour") > time_gap_hours
    } else
        condition<- !exists(variable_name) || is.null(attr(get(variable_name),"last_upd")) || time_length(now()-attr(get(variable_name),"last_upd"),unit="hour") > time_gap_hours 
    if(condition)
    {
        updated_df<-switch(variable_name,
                           "b$ans_opt"=suppressWarnings(querysql(sprintf("select * from answer_options where updated_on> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$assesm"=suppressWarnings(querysql(sprintf("select * from assessment_answers where last_updated> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$sessions100"=suppressWarnings(querysql(sprintf("SELECT * FROM class_sessions where starts_on >DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$elog"=data.table(text=ifelse(MAMP,"MAMP", "AWS elog is not downloaded")),
                           "b$qvarchive"=data.frame(text="qvarchive is not downloaded"),
                           "b$ques_log"=suppressWarnings(querysql(sprintf("select * from question_log where start_time> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$qopt"=suppressWarnings(querysql(sprintf("select * from question_options"))),
                           "b$st_tr100"= suppressWarnings(querysql(sprintf("SELECT * FROM state_transitions where transition_time>DATE_SUB(NOW(),INTERVAL %d DAY) ",transition_history))),
                           "b$ss_time"=suppressWarnings(querysql(sprintf("select * from stud_session_time where last_seen> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$stt"=suppressWarnings(querysql(sprintf("select * from stud_topic_time where last_seen> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$sindx"=suppressWarnings(querysql(sprintf("select * from student_index where last_updated> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$queries"=suppressWarnings(querysql(sprintf("select * from student_query where start_time> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$sqarchive"=data.frame(text="sqarchive is not downloaded"),
                           "b$suggo"=suppressWarnings(querysql(sprintf("select * from suggestion_options where last_updated> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$topic_log"=suppressWarnings(querysql(sprintf("select * from topic_log where transition_time> DATE_SUB(NOW(),INTERVAL %d DAY) ",history))),
                           "b$images"=suppressWarnings(querysql(sprintf("select * from uploaded_images where DateUploaded> DATE_SUB(NOW(),INTERVAL %d DAY) ",1000))),
                           
                           "alltables"=show_tbl_status(),
                           "jsonlog"=json_logs(),
                           
                           "d$questions"=suppressWarnings(querysql("SELECT * FROM questions" )),
                           "d$topic"=suppressWarnings(querysql("SELECT * FROM topic" )),
                           "d$subjects"=suppressWarnings(querysql("SELECT * FROM subjects" )),
                           "d$classes"=suppressWarnings(querysql("SELECT * FROM classes" )),
                           "d$student_query"=suppressWarnings(querysql("SELECT * FROM student_query" )),
                           "d$query_volunteer"=suppressWarnings(querysql("SELECT * FROM query_volunteer" )),
                           "d$rooms"=suppressWarnings(querysql("SELECT * FROM rooms" )),
                           "d$lesson_plan"=suppressWarnings(querysql("SELECT * FROM lesson_plan" )),
                           "d$entity_states"=suppressWarnings(querysql("SELECT * FROM entity_states" )),
                           "d$tbl_auth"=suppressWarnings(querysql("SELECT * FROM tbl_auth" )),
                           "d$school_index_map"=suppressWarnings(querysql("SELECT * FROM school_index_map" )),
                           "d$student_class_map"=suppressWarnings(querysql("SELECT * FROM student_class_map" )),
                           "d$academic_terms"=suppressWarnings(querysql("SELECT * FROM academic_terms" )),
                           "d$seating_grids"=suppressWarnings(querysql("SELECT * FROM seating_grids" )),
                           "d$seat_assignments"=suppressWarnings(querysql("SELECT * FROM seat_assignments" )),
                           "d$category"=suppressWarnings(querysql("SELECT * FROM category" )),
                           "d$elements"=suppressWarnings(querysql("SELECT * FROM elements" )),
                           
                           
                           "Missing variable in refresh function")
        updated_df_withtime<-update_time(updated_df)
    } 
    if(exists("updated_df_withtime") && nrow(updated_df_withtime)<2) {
        message(paste(variable_name,":","either zero rows found or variable cannot be refreshed due to a problem"))
        }else 
        if(exists("updated_df_withtime") && (rows<-nrow(updated_df_withtime)) >1 ) cat(paste0("\nloaded:",variable_name,":",rows," rows\n")) 
    if(exists("updated_df_withtime")) updated_df_withtime else {
        cat(".")   
        as.data.table(eval(parse(text = variable_name)))
    }
}

load_dbtables<-function() {  
    db<-dbConnect(MySQL(),user=db_user_name,password=db_password,dbname=database_name,
                  host=host_address,port=port)  # connect the DB at Amazon
    d <<- setNames(lapply(smalltables, function(t) {cat("^"); suppressWarnings(dbReadTable(db, t))}), smalltables) #load all small DB tables with variable names set as the MySQL table names. 
    d <<- lapply(d,update_time)
    cat("\n")
    dbDisconnect(db)  
}

refresh("alltables",time_gap_hours = 240)->alltables

#alltables %>% top_n(15,Data_length) %>% .$Name %>% sort() -> big15

big15<- c("answer_options" ,         "assessment_answers"  ,    "class_sessions"    ,      "event_log"      ,         "query_volunteer_archive", "question_log" ,           "question_options"   ,    
          "state_transitions"   ,    "stud_session_time"    ,   "stud_topic_time" ,       "student_index"   ,        "student_query_archive"  , "suggestion_options"   ,   "topic_log"     , "uploaded_images" )

short_names<-c( "ans_opt" ,    "assesm" ,     "sessions100" ,"elog" ,       "qvarchive" ,  "ques_log"   , "qopt" ,      "st_tr100" ,   "ss_time" ,    "stt" ,        "sindx"  ,     "sqarchive" ,  "suggo" ,     
                "topic_log",   "images")

names(big15)<-short_names

alltables %>% .$Name -> tabs
smalltables<-setdiff(tabs,big15)

if(exists("b") && ubuntu && object.size(b)<1000000) reloadDB(which = "big")
if(exists("d") && ubuntu && object.size(d)<250000) reloadDB(which = "small")

new_counter <- function() {
    i <- 0
    function() {
        # do something useful, then ...
        i <<- i + 1
        i
    }
}

countbig <- new_counter()
countsmall <- new_counter()

if(exists("b") && !ubuntu && is.list(b) && length(b)==15){
    if(length(names(b))!=15) names(b)<-short_names
    if(length(b$elog)==1) marker_b <- b$elog[1]$text else {
        reloadDB(which = "big")
        marker_b <- b$elog[1]$text
    }
    if (grepl(marker_b,"MAMP") & ! MAMP  || !grepl(marker_b,"MAMP") & MAMP) reloadDB(which="big")
}

if(exists("d") && !ubuntu && is.list(d)) {
    if(length(d$tbl_auth)==18) marker_d <- d$tbl_auth$latitude[1]
    if (marker_d == 99 & ! MAMP  || marker_d !=99 & MAMP) 
        reloadDB(which="small")
}

if(!exists("b")) {b <- list(); reloadDB(which = "big")}
if(!exists("d")) {d <- list(); reloadDB(which = "small")}



table_names<-function(search=NULL){
    if(!is.null(search) && NROW(x<-grep(search,table_names(),ignore.case = T,value=T))>0) return (x) else 
        if (is.null(search)) return(alltables$Name) else 
        return("No match found")
}


list_of_functions<-function(){
    lines<-scan(file=loaded_file,what=character(),skip = 28)
    #grepl("function",lines)
    i<-grepl("function",lines)
    fns_grouped<-{lines[i] %>% strsplit("<-")}
    listfns<-NULL
    for(i in 1: NROW(fns_grouped)){
        listfns<-c(listfns,fns_grouped[[i]][1])
    }
    sort(listfns)
}

display_image<-function(qid=NULL){
    if(!is.null(qid)){
        image_leaf=b$images[image_id==d$questions[question_id==qid,scribble_id]]$image_path
        url_image=file.path(url_images,image_leaf)
        plot(x = c(0,1200),y=c(0,800))
        content<-getURLContent(url_image)
        png_obj<-readPNG(content,info = T)
        rasterImage(png_obj,0,0,1024,768)
    }
    
}

# this function has to be fine tuned in line with the new path management
display_ipad_image<-function(ass_ansid=NULL,image_file=NULL,recency_minutes=10){
    refresh("b$assesm",time_gap_hours = recency_minutes/60) ->> b$assesm
    refresh("b$images",time_gap_hours = recency_minutes/60) ->> b$images
    
    dt1<-b$assesm[b$images,.(date=DateUploaded,assessment_answer_id,student_id,question_log_id,class_session_id,image_path),on=.(teacher_scribble_id=image_id),nomatch=0]
    
    fullpath<-file.path(url_images,ifelse(!is.null(image_file),image_file,dt1[assessment_answer_id==ass_ansid,image_path]))
    content<-getURLContent(fullpath)
    png_obj<-readPNG(content,info = T)
    plot(x = c(0,1100),y=c(0,800))
    rasterImage(png_obj,0,0,1024,768)    
}

classid<-function(session_id,recent=24){
    refresh("b$sessions100",time_gap_hours = recent)->>b$sessions100
    b$sessions100[class_session_id==session_id,class_id]
}

school<-function(school_id=NULL){
    if(!is.null(school_id) && is.numeric(school_id)) script<-paste("SELECT * from schools WHERE school_id=",school_id) else
        script<-paste("SELECT * from schools")
    querysql(script)
}

move_school<-function(user=NULL,school_id=NULL){
    old_school<-user_status(user)$school_id
    if (NROW(old_school)==0) return("USER IS INVALID")
    if(nrow(school(school_id))==0) return("SCHOOL IS INVALID") else if(NROW(old_school)==1) {
        script<-paste("UPDATE tbl_auth SET school_id= ?x WHERE user_id=?y LIMIT 1")
        SQL<-sqlInterpolate(ANSI(),script,x=school_id,y=user)
    }  else if(NROW(old_school)>1) return("The script to move more than one student at a time is still not ready")
        # if (identical(old_school,rep(school_id,NROW(old_school)))) return("ALL USERS ALREADY IN SAME SCHOOL")  else
        # {
        #     #we have problem in this code, as noquote not working..to post on StackOverflow
        #     # user_string<-paste(user,collapse=",")
        #     # script<-paste("UPDATE tbl_auth SET school_id= ?x WHERE user_id in (?y) LIMIT ?z")
        #     # SQL<-sqlInterpolate(ANSI(),script,x=school_id,y=user_string,z=NROW(old_school))
        #
        # }

    runsql(SQL)
}

school_mismatches<-function(students=F,detail=F){
    mstudents=NULL
    mteachers<-ta("schools") %>% 
        merge(ta("acad")) %>% 
        merge(ta("classes")) %>% 
        merge(ta("tbl"),by.x="teacher_id",by.y="user_id") %>% 
        select(starts_with("class"),starts_with("sch"), starts_with("acad"), 
               starts_with("user_n"),starts_with("teacher"),starts_with("sub")) %>% 
        filter(school_id.x!=school_id.y) %>% 
        arrange(class_id)
    if(students) {
        SQL1<-paste("select student_id,class_id from student_class_map order by student_id")
        x<-querysql(SQL1) %>% merge(ta("tbl_"),by.x="student_id",by.y="user_id")

        y<-ta("schools") %>% 
            merge(ta("acad")) %>% 
            merge(ta("classes"))
            
        mstudents<-left_join(x,y,by="class_id") %>%  select(starts_with("stude"),user=user_name, school_id.x, 
                                                 school_id.y, starts_with("class"),starts_with("acad")) %>% 
            filter(school_id.x!=school_id.y)
    }
    mismatched<-list(Teachers=mteachers,Students=mstudents)
if(!detail) mismatched<-list(Mismatched_Teacher_classes=mteachers$class_id,Mismatched_teachers=mteachers$teacher_id,Mismatched_students=unique(mstudents$user),Mismatched_student_classes=mstudents$class_id)
    mismatched
}

stud_mismatches<-function(){
    assesm %>% group_by(class_session_id) %>% count(studcnt_answers=n_distinct(student_id)) ->assessm_attend
    ss_time %>% group_by(class_session_id) %>% summarise(present=sum(present),count_sstime=n())->sstime_attendance
    left_join(sessions100,sstime_attendance) %>% left_join(assessm_attend)->attendance_merged
    attendance_merged %>% select(class_session_id,class_id,teacher_id,total_stud_registered,stud_attended,present,count_sstime,studcnt_answers,session_state,session_time,starts_on,ends_on,n)
}

insert_class<-function(class_name=NULL,grade=NULL,section=NULL,teacher_id=NULL,acad_term=NULL,subject=NULL){
    script<-"INSERT INTO classes (class_name,grade_id,section_id,subject_id,academic_term_id,teacher_id) VALUES (?name, ?grade, ?sec, ?sub, ?acad, ?t )"
    script<-sqlInterpolate(ANSI(),script,name=class_name,grade=grade,sec=section,sub=subject,acad=acad_term,t=teacher_id)
    runsql(script)
}

insert_subj<-function(school_id=NULL,subject_name=NULL){
    script<-"INSERT INTO subjects (school_id,subject_name) VALUES (?sc, ?sub)"
    script<-sqlInterpolate(ANSI(),script,sc=school_id,sub=subject_name)
    runsql(script)
}

insert_acad_term<-function(school_id=NULL,text=NULL,startdate=date(now()),enddate=date(now()+ddays(90))){
    script<-"INSERT INTO academic_terms (school_id,description,start_date,end_date) VALUES (?sc, ?de, ?sd, ?ed )"
    script<-sqlInterpolate(ANSI(),script,sc=school_id,de=text,sd=as.character(startdate),ed=as.character(enddate))
    runsql(script)
    querysql(paste("SELECT * FROM academic_terms where school_id=",school_id))
}

delete_acad_term<-function(acad_id=NULL){
    script<-paste("DELETE FROM academic_terms WHERE academic_term_id=",acad_id, "LIMIT 1")
    runsql(script)
}

change_subject<-function(subj=NULL,class=NULL){
    if(!is.null(subj) && !is.null(class)) script<-"UPDATE classes SET subject_id = ?sub WHERE class_id= ?cl LIMIT 1" else return("error")
    script<-sqlInterpolate(ANSI(),script,sub=subj,cl=class)
    runsql(script)
}

change_teacher<-function(class=NULL,teacher=NULL){
    if(!is.null(class) && !is.null(teacher)) script<-"UPDATE classes SET teacher_id = ?t WHERE class_id= ?cl LIMIT 1" else return("error")
    script<-sqlInterpolate(ANSI(),script,cl=class,t=teacher)
    runsql(script)
}

change_class_name<-function(classid=NULL,new_name=NULL){
    if(!is.null(classid) && !is.null(new_name)) script<-sprintf("UPDATE classes SET class_name = '%s' WHERE class_id= %d LIMIT 1",new_name,classid) else return("error")
    runsql(script)
}

map_academic_term<-function(acad_term=NULL, class=NULL,school=NULL){
    if(is.null(acad_term)) return("Academic Term id is mandatory")
    
    if(is.null(class) && !is.null(school)) {
        at<-ta("acad") %>% filter(school_id==school)
        if(acad_term %in% at$academic_term_id) return("This is already mapped. No changes affected in DB") else
           { script<-"UPDATE academic_terms SET school_id = ?sch WHERE academic_term_id = ?a LIMIT 1"
             script<-sqlInterpolate(ANSI(),script,sch=school,a=acad_term)
           }
    } 
    if(!is.null(class) && is.null(school)) {
        classes<-ta("classes") %>% filter(academic_term_id==acad_term)
        if (class %in% classes$class_id) return("This class is already mapped to the correct school. No changes affected in DB") else 
        {
            script<-"UPDATE classes SET academic_term_id = ?a WHERE class_id= ?cl LIMIT 1"
            script<-sqlInterpolate(ANSI(),script,a=acad_term,cl=class)
        }
    }
    if(!is.null(class) && !is.null(school)) return("One extra argument. Expecting 2 and got 3")
    
    runsql(script)
}

class_list<-function(stud=NULL){
    scm<-ta("student_class_map")
    x<-scm %>% filter(student_id %in% stud) %>% group_by(student_id) %>% summarise(total_classes=n())
    y<-scm %>% filter(student_id %in% stud) %>% select(student_id,class_id)
}

valid_user<-function(user=NULL) if(user  %in% d$tbl_auth$user_id) T else F

valid_teacher<-function(user=NULL) if(nrow(d$tbl_auth %>% filter(user_id==user,role_id==3))>0) T else F

valid_school<-function(sc=NULL) if(nrow(d$schools %>% filter(school_id %in% sc))==1) T else F

recent_topic_logs<-function(recency=1){
  refresh("topic_log",time_gap_hours = recency)->topic_log
topic_log %>% arrange(desc(Auto_id)) %>% as.data.table()
}

recent_ques<-function(minutes=1){
    refresh("b$ques_log",time_gap_hours = minutes/60)->>b$ques_log
    b$ques_log[d$questions,.(question_id,question_name,type=question_type_id,datetime=start_time,teacher_id),on="question_id"][order(-datetime)] %>% head(20)
}

ques_usage<-function(){
  b$ques_log[,{
    tot=.N
    ques_type=d$questions[question_id==.BY[[1]],question_type_id]
    .(type=ques_type,total=tot)
  },by="question_id"]
}

ans<-function(){
  b$ques_log[b$assesm,
           .(qid=question_id,qlog=question_log_id,ass=assessment_answer_id,stud=student_id,score=percent(answer_score,2),mod=model_answer,sess=class_session_id),
           on="question_log_id"
           ]
}

ans2<-function(){
  ans()[ques(),on=.(qid),nomatch=0]
}



start_classes<-function(number_rows=1,start_after=1,
                        duration=0.5, class_id=14,room_id=25,teacher_id=496,daily=F,gaps = 0.5){
    script_array<-gsinsert_class_sessions(number_rows,start_after,
                                          duration, class_id,room_id,teacher_id,daily=daily,gaps = 0.5)
    for(i in seq_along(script_array)){
        runsql(script_array[i])
    }
}

add_student<-function(class_id=NULL,students=NULL){
    for (i in students){
        script<-"INSERT INTO student_class_map (student_id,class_id,last_updated) VALUES (?id1, ?id2, ?id3 ) "
        script_new<-sqlInterpolate(ANSI(),script,id1=i,id2=class_id,id3=as.character(now()))
        runsql(script_new)
    }
}

remove_student<-function(class_id=NULL,students=NULL){
  for (i in students){
    script<-"DELETE FROM student_class_map WHERE class_id=?class AND student_id=?stud"
    script_new<-sqlInterpolate(ANSI(),script,class=class_id,stud=i)
    runsql(script_new)
  }
}

common_students<-function(class1=NULL,class2=NULL){
    ids<-intersect(students(class1)$student_id,students(class2)$student_id)
    user_status(ids)
}

user<-function(name="Edna"){
table_all("tbl_auth") %>% filter(grepl(name,ignore.case = T, user_name))
}

change_room<-function(greater_than=NULL,room_id=NULL,limit=10){
    script<-"UPDATE class_sessions SET room_id=?x where class_session_id>?y limit ?li"
    script<-sqlInterpolate(ANSI(),script,x=room_id,y=greater_than,li=limit)
    runsql(script)
}

room_efficiency<-function(summary=T) {
    refresh("b$sessions100")->>b$sessions100
    output<-rooms()[b$sessions100,
                    {
                        sess=class_session_id;
                        date=date(starts_on);
                        room=room_id;
                        regis=total_stud_registered;
                        seat=seat_counts;
                        tot=total_stud_registered;
                        class=class_id;
                        teacher=teacher_id
                        .(session=sess,class=class,teacher=teacher,room=room,date=date,stud=regis,seat=seat,perc_seats_used=percent(tot/seat))
                    },
                    on="room_id",nomatch=0][order(-perc_seats_used)]
    if(summary) output<-output[,.(maxseat=max(seat),max_stud=max(stud),utiliz=percent(max(stud)/max(seat))),by=c("class","room","teacher")][order(class,room,teacher)]
    output
}

best_room<-function(t=499){
dt1<-room_efficiency()
dt1[,best_room := .SD[order(-utiliz)]$room[1], by=class
    ][,best_seat:=.SD[room==best_room,maxseat][1],by=class
      ][teacher==t,.(room=first(best_room),stud=first(max_stud),seats=first(best_seat),utli=max(percent(max_stud/best_seat)),teacher=.(unique(teacher))),by=class]
}

valid_rooms<-function(singclass=NULL){
    classes()[,.(class_id,class_name,stud_regist)
              ][rooms(),.(room_id,seat_counts,class_id,class_name),by=.EACHI,on="stud_regist<=seat_counts"
                ][class_id == singclass][,room_id]
}

room_to_classes<-function(uniform=F,reverse=F){ # this function is useless. Ignore. Will be deleted soon
    tt=c(uniform,reverse)
    if(identical(tt,c(T,T)))
        output<-classes()[,.(class_id,class_name,teacher_id)
                  ][b$sessions100,on="class_id"
                    ][,.(rooms=sort((unique(i.room_id)))),by=class_id][order(class_id)]

    if(identical(tt,c(F,T)))
        output<-classes()[,.(class_id,class_name,teacher_id)
                  ][b$sessions100,on="class_id"
                    ][,.(listrooms=.(sort((unique(i.room_id))))),by=class_id][order(class_id)]
        
    if(identical(tt,c(F,F)))
        output<-classes()[,.(class_id,class_name,teacher_id)
              ][b$sessions100,on="class_id"
                ][,c(1,3,4,5,6,7,8)
                  ][,.(listclasses=.(sort((unique(class_id))))),by=.(room=i.room_id)][order(room)]
    if(identical(tt,c(T,F)))
        output<-classes()[,.(class_id,class_name,teacher_id)
                  ][b$sessions100,on="class_id"
                    ][,c(1,3,4,5,6,7,8)
                      ][,.(class=sort((unique(class_id)))),by=.(room=i.room_id)][order(room)]
    
    output
}

seat_info<-function(session=NULL){
    #below script is used in PHP
    #script<- "select seat.seat_id, assign.seat_state from seats as seat inner join class_sessions as session on session.room_id = seat.room_id left join seat_assignments as assign
    #on seat.seat_id = assign.seat_id and assign.class_session_id = ?x left join entity_states as state on session.session_state = state.state_id where session.class_session_id = ?x";
    
    #new script
    script<-"select seat_id, seat_state from seat_assignments where class_session_id=?x"
    script<-sqlInterpolate(ANSI(),script,x=session)
    querysql(script)
}

gsinsert_class_sessions<-function(number_rows=1,start_after=1,
                                   duration=0.5, class_id=14,room_id=25,teacher_id=496,daily=F,gaps = 0.5){
    if(daily){
        value_start <- seq(from = now()+dhours(start_after),to =now()+dhours(start_after)+dhours(number_rows*24),by=dhours(24))
        value_end <-  seq(from = now()+dhours(start_after)+dhours(duration),to =now()+dhours(start_after)+dhours(duration)+dhours(number_rows*24),by=dhours(24))
    }
    else{
        value_start <- now()+dhours(seq(from=start_after,length.out = number_rows,by = duration+gaps))
        value_end <- now()+dhours(seq(from=start_after+duration,length.out = number_rows,by = duration+gaps ))
    }
    query_text<-"INSERT INTO class_sessions (class_id,room_id,teacher_id,starts_on,ends_on,session_state)"
    values<-sprintf("VALUES(%d,%d,%d,'%s','%s',%d)", class_id,room_id,teacher_id,value_start,value_end, 4)
    paste(query_text,values)
}

shift_session<-function(session=NULL,min=5,what="both"){
  x<-show_sessions() %>% filter(session_state<6 & class_session_id==session)
  if(nrow(x)==1){
    new_end_time<-switch(what,both=x$ends_on + dminutes(min),end=x$ends_on + dminutes(min),x$ends_on)
    new_start_time<-switch(what,both=x$starts_on + dminutes(min),start=x$starts_on + dminutes(min),x$starts_on)
    script<-"UPDATE class_sessions SET starts_on=?nst, ends_on= ?net WHERE class_session_id= ?id LIMIT 1"
    new_script<-sqlInterpolate(ANSI(),script,nst=as.character(new_start_time),net=as.character(new_end_time),id=session)
    runsql(new_script)
  }else return("This session is either missing today or not in 4,2 or 1 state")
}

auto_cancel<-function(days_old=1,recent=1){
    refresh("d$tbl_auth",time_gap_hours = recent) ->> d$tbl_auth
  sess1<-show_sessions(days = days_old) %>% filter(session_state %in% c(1,2) & ends_on <now())
  sess4<-show_sessions(days = days_old) %>% filter(session_state ==4 & ends_on <now())
  #stud_dflist<-lapply(X = sess1$class_id,FUN = students)
  #teach_vector<- bind_rows(sess1,sess4) %>% select(teacher_id)
  #student and teacher states are NOT changed.
  #all_students<-Reduce(bind_rows,stud_dflist) %>% filter(user_state %in% c(1,9,10))
  s1<-sprintf("update class_sessions set session_state=5 where class_session_id=%d ",sess1$class_session_id)
  s4<-sprintf("update class_sessions set session_state=6 where class_session_id=%d ",sess4$class_session_id)
  sdel<-sprintf("delete from live_session_status where session_id = %d",sess1$class_session_id)
  #schg<-sprintf("update tbl_auth set  user_state = 7 where user_id = %d",all_students$student_id)
  sql_array<-list(s1,s4,sdel)
  lapply(sql_array,function(x){ if (length(x)>0) lapply(x,runsql)});
  #runsql(schg)
  #message("Executed following scripts:")
  #print(sql_array)
}

replace_null_date<-function(table_name=NULL,date_name=NULL,key_name=NULL){
  # this is not yet working
  dump<-querysql(sprintf("select %s,%s from %s",key_name,date_name,table_name))
  na_rows<-dump %>% filter(is.na(eval(date_name)))
  if (nrow(na_rows)>0) 
    na_rows %>% count(eval(key_name)) %>% 
    left_join(dump,by=eval(key_name)) %>% 
    filter(complete.cases(eval(key_name)))
}


show_sessions2<-function(number=10){
show_sessions(number=number) %>% merge(table_all("classes"),by = "class_id") %>% select(1:8,14,17,20) %>% merge(table_all("subjects"))
}

students2<-function(class_id=NULL,session=NULL){ # delete this once everything is fine
    if(!is.null(class_id)) querysql(paste('SELECT student_id FROM student_class_map WHERE class_id=',class_id)) %>% 
        merge(d$tbl_auth,by.x="student_id",by.y="user_id") %>% 
        select(student_id,first_name,last_name,grade_id,user_state,school_id) else
    if(!is.null(session)) {
        script<- "select map.student_id from student_class_map as map inner join class_sessions as session on map.class_id = session.class_id where session.class_session_id = ?x"
        script<-sqlInterpolate(ANSI(),script,x=session)
        querysql(script) %>% 
            merge(d$tbl_auth,by.x="student_id",by.y="user_id") %>% 
            select(student_id,first_name,last_name,grade_id,user_state,school_id)
    } else "Incorrect input"
}

students<-function(class_id=NULL,session=NULL,refresh=F){
    cl_id<-class_id
    if(refresh) refresh("d$student_class_map",time_gap_hours = 0) ->> d$student_class_map
    if(!is.null(class_id)) {
         outp<- d$tbl_auth[d$student_class_map[class_id==cl_id],.(student_id,first_name,last_name,grade_id,user_state),on=.(user_id=student_id)] 
        }else
            if(!is.null(session)) {
                outp<- students(classid(session_id=session,recent = ifelse(refresh,0,24)),refresh = refresh)
            } else "Incorrect input"
    outp
}

stud_regist<-function(class="ALL"){
  all_classes<-querysql(paste('SELECT class_id,count(student_id) as count FROM student_class_map GROUP BY class_id'))
  if (length(class)==1 && grepl("A",class,ignore.case = T)) all_classes else
      all_classes %>% filter(class_id %in% class)
}

upd_stud_regist<-function(){
    all_classes<-querysql(paste('SELECT class_id,count(student_id) FROM student_class_map GROUP BY class_id'))
    script<-sprintf("UPDATE classes SET stud_regist = %d where class_id=%d",all_classes$count,all_classes$class_id)
    for (i in seq_along(script))
        runsql(script[i])
}

live_status<-function(){
  lv<-list(topic=NULL,sessions=NULL,users=NULL)
  x<-ta("live")
  y<-ss() %>% filter(session_state==1)
  z<-list_users() %>% filter(user_state==1)
  if(nrow(x)>0) x-> lv[[1]]
  if(nrow(y)>0) y %>% select(1:6) ->lv[[2]]
  if(nrow(z)>0) z %>% select(1,5,7:11) ->lv[[3]]
  lv
}

remove_ended<-function(){
    lss<-ta("live_session_status")
    sessions_to_check<-paste(lss$session_id,collapse = ",")
    if(sessions_to_check!='') extracted<-querysql(sprintf("SELECT class_session_id, ends_on FROM class_sessions WHERE class_session_id in (%s)",sessions_to_check)) else
        extracted<-NULL
    if(!is.null(extracted)) {
        to_be_deleted_sessions<-
    {extracted %>% filter(ends_on<now()) %>% select(class_session_id)}$class_session_id
    to_be_deleted_sessions_comma<-paste(to_be_deleted_sessions,collapse = ",")
    runsql(sprintf("DELETE FROM live_session_status WHERE session_id in (%s)",to_be_deleted_sessions_comma))
    } else 0
}

role<-function(user=NULL) list_users()[user_id==user,role_id]

classes<-function(user=NULL,school="ALL",detail=T,refreshNow=F){
    d$tbl_auth<<-refresh("d$tbl_auth",time_gap_hours = 24)
    d$classes<<-refresh("d$classes",time_gap_hours = ifelse(refreshNow,0,24))
    d$subjects<<-refresh("d$subjects",time_gap_hours = 24)
    d$academic_terms<<-refresh("d$academic_terms",time_gap_hours = 240)
    r<-role(user)
    if(length(r)==0) r<-0
    if (r==3){
        if(detail) class_list<- classes()[teacher_id==user] else
            class_list<-  classes()[teacher_id==user][,class_id]
    } else
        if (r==4)
        {
            as.data.table(d$student_class_map)[student_id==user,class_id] -> class_list
            if(detail) class_list<-classes()[class_id %in% class_list] else
                class_list<-classes()[class_id %in% class_list][,class_id]
        } else 
            if(valid_school(school)) {
                valid_acad_terms<-as.data.table(d$academic_terms)[school_id==school,academic_term_id]
                class_list<- classes()[academic_term_id %in% valid_acad_terms]
            } else 
            {class_list<-
                d$tbl_auth[d$classes,on=c(user_id="teacher_id")][
                    d$subjects,.(class_id,class_name,school_id,teacher_id=user_id, TeacherName=user_name, grade=i.grade_id,section=i.section_id,academic_term_id, 
                                 stud_regist,subject_id,subject_name,GI=percent(gi_num/gi_den),pi),on="subject_id",nomatch=F][order(class_id)]
            #select(starts_with("class"),school_id,teacher_id, TeacherName=user_name, grade_id,section_id, stud_regist,starts_with("subj"),gi_num,gi_den,pi) %>%
            if(!detail) class_list<-class_list$class_id
            }
    cat("\n")
    class_list
}

valid_class<-function(class){
  if(NROW(class)==1 && nrow(classes() %>% filter(class_id==class))==1) T else F
}

cl<-function(pattern="NULL"){
  classes() %>% filter(grepl(pattern,class_name))
}

assign_class2teacher<-function(class=NULL,teacher=NULL){
  if(all(!is.null(teacher),!is.null(class))){
    if(valid_teacher(teacher)&& valid_class(class)) {
    script<-"UPDATE classes SET teacher_id=?x WHERE class_id=?y LIMIT 1"
    SQL<-sqlInterpolate(ANSI(),script,x=teacher,y=class)
    runsql(SQL)
    } else message("Either teacher is invalid or class is invalid")
  } else message("Either teacher is missed or class : Both class & teacher are mandatory inputs")
}

assign_class2student<-function(class=NULL,students=NULL){
  if(all(!is.null(students),!is.null(class))){
    x<-paste0("(",students);   y<-paste0(class,")");   value_list<-paste0(x,",",y,collapse=",")
    script<-"INSERT INTO student_class_map (student_id,class_id) VALUES ?x ON DUPLICATE KEY UPDATE class_id= ?y "
    SQL<-sqlInterpolate(ANSI(),script,x=value_list,y=class)
    #runsql(SQL)
    SQL
  } else message("Both student and class parameters are mandatory")
}

class_conflict<-function(classes=1:20){
  for (i in classes){
  }
}

rooms<-function(){
    d$seating_grids<<-refresh("d$seating_grids",time_gap_hours = 24)
    grids<-copy(d$seating_grids)
    grids[,rcount := str_count(string = seats_removed,pattern="A")] # count number of 'A's
    grids[,seat_counts:= seat_rows*seat_columns - rcount] # subtract seats removed
    grids[,.(room_id,row=seat_rows,col=seat_columns,seat_counts)][order(room_id)]
}

list_XMPP_users<-function() {
    script<-"SELECT * FROM users"
    x<-suppressWarnings( querysql(script,database="ejabberd_1609"))
    x[,1:2]
}

# this function may not be used now. Use the new ques() function and join it with ans() or ans2()
questions<-function(pattern=NULL,topic=NULL,hrs=24,question=NULL,subj=NULL,.subj=NULL,width=getOption("width")-100){
    d$classes<<-refresh("d$classes",time_gap_hours = hrs)
    d$questions<<-refresh("d$questions",time_gap_hours = hrs)
    d$topic<<-refresh("d$topic",time_gap_hours = hrs)
    d$subjects<<-refresh("d$subjects",time_gap_hours = hrs)
    d$lesson_plan<<-refresh("d$lesson_plan",time_gap_hours = hrs)
    if(!is.null(subj)) {
        qids<-d$questions %>% left_join(d$topic,by="topic_id") %>% filter(subject_id==subj) %>% select(question_id) %>% .[,1]
        subj_name<-d$subjects %>% filter(subject_id==subj) %>% select(subject_name) %>% .[,1]
        return(questions(question = qids,.subj = subj_name))
    }
    tt<-c(is.null(pattern),is.null(topic),is.null(question))
    if(identical(tt,c(T,T,F))) filtered_list<-d$questions %>% filter(question_id %in% question)
    if(identical(tt,c(T,F,T))) filtered_list<-d$questions %>% filter(topic_id %in% topic)
    if(identical(tt,c(F,F,T))) filtered_list<-d$questions %>% filter(topic_id %in% topic,grepl(pattern,question_name,ignore.case = T))
    if(identical(tt,c(F,T,T))) filtered_list<-d$questions %>% filter(grepl(pattern,question_name,ignore.case = T))
    
    if(exists("filtered_list")) {
        count_classes<-filtered_list %>% left_join(d$lesson_plan,by="topic_id") %>% left_join(d$classes[,1:7],by="class_id") %>% 
        group_by(question_id) %>% 
            summarise(no_classes=n(),class_list=paste(class_id,collapse = ","),teachers=paste(unique(teacher_id.y),collapse=","),class_sub=paste(unique(subject_id),collapse=","))
        filtered_list<-count_classes %>% left_join(filtered_list,by="question_id") 
    } 
    if (exists("filtered_list")){ 
        detailed_list<- filtered_list %>%  left_join(d$topic[,c("topic_id","topic_name","parent_topic_id")],by="topic_id") %>% 
            left_join(d$topic[,c("topic_id","topic_name")],by=c("parent_topic_id"="topic_id")) %>% 
            mutate(question_text=str_trunc(question_name,w=width,side = "right")) %>% 
            select(question_id,question_text,parent_topic_id,parent_topic_name=topic_name.y, topic_id,subtopic_name=topic_name.x,question_type_id,class_list,teachers,class_sub) 
        if(nrow(detailed_list)>0) detailed_list$subject_name=.subj else return (message("Warning: there are no questions to display in this subject?"))
        detailed_list
    } else return (message("Error: please check parameter sequence; are you giving too many or too few parameters?"))
}

replace_question<-function(question_id=NULL,text=NULL){
  script<-"UPDATE questions SET question_name=?y WHERE question_id=?x LIMIT 1"
  SQL<-sqlInterpolate(ANSI(),script,x=question_id,y=text)
  runsql(SQL)
}

replace_question_option<-function(option_id=NULL,text=NULL){
  script<-"UPDATE question_options SET question_option=?y WHERE question_option_id=?x LIMIT 1"
  SQL<-sqlInterpolate(ANSI(),script,x=option_id,y=text)
  SQL
}

replace_blank_qo<-function(){ # an interactive script  - will ask before replacing so go ahead and fire it,
  b$qopt[question_option=="",question_option_id]->blank_options
  stopifnot(length(blank_options)>0)
  message(paste("Detected",length(blank_options),"blank options"))
  print(blank_options)
  X <- readline(prompt = "Enter a unique replacement string:")
  section1 <- "UPDATE question_options SET question_option = CASE\n"
  section2 <-    sprintf("WHEN question_option_id= %d THEN '%s'",blank_options,paste0(X,seq_along(blank_options)))
  section2 <- paste(section2,collapse = "\n")
  section3<- sprintf("END\nWHERE question_option_id IN (%s)",paste(blank_options,collapse = ","))
  script<-paste(section1,section2,section3)
  print(script)
  Val <- readline("Validate the script and enter Y if you want to proceed, else any other key:")
  stopifnot(Val=="Y")
  message(paste("Updated",runsql(script),"rows of question_options. Please refresh b$qopt"))
}

replace_topic<-function(topic_id=stop("Topic_id cannot be null"),text=stop("Text cannot be null")){
  script<-"UPDATE topic SET topic_name=?y WHERE topic_id=?x LIMIT 1"
  SQL<-sqlInterpolate(ANSI(),script,x=topic_id,y=text)
  runsql(SQL)
}

show_quest<-function(qid=NULL){
   main_part<- d$questions[question_id==qid,.(type=question_type_id,title=question_name,last_updated)]
   if(main_part$type %in% 1:2)
     opts<-b$qopt[question_id==qid,.(question_option,is_ans=is_answer)] else opts="Not yet filled"
   #if(main_part$type %in% 3)
   if(main_part$type %in% 4) opts <- display_image(qid)
   #if(main_part$type %in% 5)
   #if(main_part$type %in% 6 ) 
   main_part$type <- d$question_types[question_type_id==main_part$type,question_type_title]
   ques_det<-list(main=main_part,options=opts)
   ques_det[]
}


pic<-function(path=NULL){
  download.file(file.path("http://54.251.104.13/images",path),'~/Downloads/SM001.png')
  img <- readPNG('~/Downloads/SM001.png')
  grid::grid.raster(img)
}

ques<-function(pattern="",topic=1:1000,w=getOption("width")){
    if(is.numeric(topic)) topics <-topics(topic_range = topic) else topics <-topics(pattern = topic)
    option_counts<- b$qopt[d$questions,.(options=.N),on="question_id",by=question_id][!is.na(question_id)]
    option_counts[d$questions,on="question_id",nomatch=NA][
      topics,on="topic_id"][grepl(pattern,question_name,ignore.case = T)][
        ,.(qid=question_id,topic_id,parent=parent_topic_id,scr=scribble_id,OTF=on_the_fly,
           type=question_type_id,options,ques=str_trunc(question_name,width=w-60,side="center"))][
             order(qid)
           ]
}

mrq<-function(q_id=NULL){
    question_det<- ques(w=500)[qid==q_id]
    outp<-list()
    outp$question <- question_det
    outp$options <- b$qopt[question_det,.(opt_id=question_option_id,question_option,is_ans=is_answer,last_updated),on=.(question_id=qid),nomatch=0]
    outp
}

add_mrq1<-function(quest="",options=data.frame(),topic_id=NULL,userid=499){
    scr1 <- sprintf("INSERT INTO questions (question_type_id, topic_id, teacher_id, classification,question_name, on_the_fly ) VALUES (2,%d,%d,1,'%s',0)",topic_id,userid,quest)
    runsql(scr1)
    scr2 <- sprintf("SELECT question_id from questions where question_type_id=2 AND topic_id=%d AND question_name= '%s'",topic_id,quest)
    querysql(scr2)
}

add_mrq_options <-function(){ # complete this with returned value from above
}

add_ques<-function(quest="Dummy text",type=6,options=data.frame(),topic_id=NULL,userid=499){
    scr1 <- sprintf("INSERT INTO questions (question_type_id, topic_id, teacher_id, classification,question_name, on_the_fly ) VALUES (%d,%d,%d,1,'%s',0)",type,topic_id,userid,quest)
    runsql(scr1)
    scr2 <- sprintf("SELECT question_id from questions where question_type_id=%d AND topic_id=%d AND question_name= '%s'",type,topic_id,quest)
    querysql(scr2)
}
queries<-function(n=10){
    script<-paste("select * from student_query order by query_id desc limit", n)
    q<-suppressWarnings(querysql(script))
    q
}

changestate<-function(user=NULL,state_user=NULL,session=NULL,state_session=NULL){
    if(all(!is.null(user),!is.null(state_user))){
      old_user_state<-user_status(user)$user_state
    string1<-paste("Existing user state:",old_user_state)
    string2<-paste("New user state:",state_user)
    script<-paste("UPDATE tbl_auth SET user_state=",state_user,"WHERE user_id=",user,"LIMIT 1")
    n<-runsql(script)
    if (n==1) {print(string1); print(string2)} else print("No state changes affected on user")
    }
    if(all(!is.null(session),!is.null(state_session))){
        old_sess_state<-show_sessions(days = 7) %>% filter(class_session_id==session) %>% select(session_state) %>% .[,1]
        string1<-paste("Existing session state:",old_sess_state)
        string2<-paste("New session state:",state_session)
        script<-paste("UPDATE class_sessions SET session_state=",state_session,"WHERE class_session_id=",session,"LIMIT 1")
        n<-runsql(script)
        if (n==1) {print(string1); print(string2)} else print("No state changes applied on Session")
    }
}

delete_session<-function(session_id=stop("you have to provide the numeric session_id")){
    script<-paste("DELETE from class_sessions where class_session_id=",session_id)
    deleted_sessions<-runsql(script,database_name)
    message(deleted_sessions,":rows deleted from class_session_id")
}

list_users<-function(school="ALL",refresh=F) {
    if(refresh) refresh(variable_name = "d$tbl_auth",time_gap_hours = 0)->> d$tbl_auth else
        refresh(variable_name = "d$tbl_auth",time_gap_hours = 24)->> d$tbl_auth 
  if(NROW(school)==1 && school=="ALL") (d$tbl_auth) else if (valid_school(school)) d$tbl_auth[school_id==school] else
        "INVALID school_id"
}

state_transitions<-function(time_gap_minutes=1){
    refresh("b$st_tr100",time_gap = time_gap_minutes/60)->>b$st_tr100
    cat("\n")
    b$st_tr100[order(-id)]
}

delete_log<-function(time=NULL){
    script<-"DELETE FROM state_transitions WHERE transition_time= ?t LIMIT 1"
    sql<-sqlInterpolate(ANSI(),script,t=time)
    runsql(sql)
}

password_mismatches<-function() {
    xmpp_users<-list_XMPP_users()
    users<-list_users()
    users$user_id <- as.character(users$user_id)
    xmpp_users[users,on=.(username=user_id)][password!=i.password]
}

password_sync<-function() {
    x<-password_mismatches()
    sql_text<-paste0("UPDATE users SET password=", "'" ,x$password.y, "'"  ,  " WHERE username=", x$username, ";")
    number_updated<-runsql(sql_text,"ejabberd_1609")
     cat(paste(number_updated,"password(s) updated in XMPP server"))
    #cat(sql_text)
}

users_missed<-function() {
    x<-list_XMPP_users() %>% select("username")
    y<-list_users()[,user_id]
    message("Users in xmpp and NOT in API server:") 
    print(setdiff(x$username,y))
    message("Users in API and NOT in XMPP server:") 
    setdiff(y,x$username)
}

class_details<-function(classid=1,names=T,tag=T){
    dt1<- d$topic[d$lesson_plan,.(class_id,topic_id,topic_tagged),on="topic_id"][
        ,.(topics=list(topic_id),tags=list(topic_tagged),count=.N,tagged=sum(topic_tagged)),by=class_id][
            d$classes,.(teacher_id,class_id,subject_id,topics,count,tagged),on="class_id"]
    topic_numbers<-dt1[class_id==classid,topics][[1]]
    topics_tagged<-d$lesson_plan[topic_id %in% topic_numbers & class_id== classid & topic_tagged>0,topic_id] # assuming we still consider tags 1,2,3.. as tagged and only 0 is untagged
    topics_zero_tagged<- d$lesson_plan[topic_id %in% topic_numbers & class_id== classid & topic_tagged==0,topic_id]
    topics_main<- d$topic[topic_id %in% topic_numbers & is.na(parent_topic_id),topic_id]
    sub<-dt1[class_id==classid,subject_id]
    subjectname<-d$subjects[subject_id==sub,subject_name]
    t<-dt1[class_id==classid,teacher_id]
    teacher<-d$tbl_auth[user_id==t,.(first_name,last_name,user_name)]
    top_names<-d$topic[topic_id %in% t,topic_name]
        outp<- list(teacher=teacher,subject=subjectname,topics=topic_numbers,main=topics_main)
        if(names) {
            outp$topics <- d$topic[topic_id %in% topic_numbers,topic_name]
            outp$main <- d$topic[topic_id %in% topics_main,topic_name]
        }
        if(tag & names) {
            outp$tagged <- d$topic[topic_id %in% topics_tagged,topic_name]
            outp$not_tagged <- d$topic[topic_id %in% topics_zero_tagged,topic_name]
        }
        if(tag & !names) {
            outp$tagged <- topics_tagged
            outp$not_tagged <- topics_zero_tagged
        }
    return(outp)
}

main_topics_inClass<-function(class=2) {
    subtcounts<-d$topic[!is.na(parent_topic_id),.N,by="parent_topic_id"]
    maintopics_all()[topic_id %in% class_details(class,names = F)$main
                     ][subtcounts,.(topic_id,main_topic=topic_name,subtopic_counts=N),on=.(topic_id=parent_topic_id),nomatch=0]
}

subtopics<- function(main=NULL,subj=NULL) { # completely overhauled so that we have no separate id and pattern paramters
    tt<- c(!is.null(main),!is.null(subj))
    if(identical(tt,c(T,F))){
        if(is.numeric(main)) outp<- subtopics_all()[parent_topic_id==main] else   
            outp<-subtopics_all()[grepl(main,Parent_topic,i=T)]
    } else  
        if(identical(tt,c(F,T))){
            if(is.numeric(subj)) outp<- subtopics_all()[subject_id==subj] else   
                outp<-subtopics_all()[grepl(subj,subject_name,i=T)]
        }
    outp
}


main_topics<-function(subpat=NULL,toppat=NULL){
    tt<-c(is.null(subpat),is.null(toppat))
    t<-suppressWarnings(querysql("SELECT * from topic"))
    s<-suppressWarnings(querysql("SELECT * from subjects"))
    if(identical(tt,c(T,T))) t %>% filter(is.na(parent_topic_id)) %>% merge(s) %>% rename(MainTopic=topic_name) %>% select(subject_name,subject_id,MainTopic,topic_id) %>% arrange(subject_id,topic_id) else
        if(identical(tt,c(T,F))) t %>% filter(is.na(parent_topic_id)) %>% merge(s) %>% rename(MainTopic=topic_name) %>% select(subject_name,subject_id,MainTopic,topic_id) %>% filter(grepl(toppat,MainTopic,ignore.case = T)) %>% arrange(subject_id,topic_id) else
        if(identical(tt,c(F,T))) t %>% filter(is.na(parent_topic_id)) %>% merge(s) %>% rename(MainTopic=topic_name) %>% select(subject_name,subject_id,MainTopic,topic_id) %>% filter(grepl(subpat,subject_name,ignore.case = T)) %>% arrange(subject_id,topic_id) else
        if(identical(tt,c(F,F))) t %>% filter(is.na(parent_topic_id)) %>% merge(s) %>% rename(MainTopic=topic_name) %>% select(subject_name,subject_id,MainTopic,topic_id) %>% filter(grepl(subpat,subject_name,ignore.case = T),grepl(toppat,MainTopic,ignore.case = T)) %>% arrange(subject_id,topic_id)
}

subtopics_all<-function(){
    t2<-copy(d$topic)
    t2[d$topic,.(topic_id, topic_name,Parent_topic=i.topic_name,parent_topic_id,subject_id), on= .(parent_topic_id=topic_id),nomatch=0][d$subjects,on="subject_id"]
}

maintopics_all<-function(){
    d$topic[d$subjects,on="subject_id"][is.na(parent_topic_id),.(topic_id,topic_name,subject_id,subject_name)]
}


topics<-function(pattern="",topic_range=1:1000){
    d$topic[topic_id %in% topic_range & grepl(pattern,topic_name,i=T),.SD,.SDcols=c(1,2,3,5)]
}

cols<-function(table="class_sessions", dbname=database_name,exact=F){
    db<-DBI::dbConnect(RMySQL::MySQL(),user=db_user_name,password=db_password,dbname=dbname,
                       host=host_address,port=port)  # connect the DB
    on.exit(dbDisconnect(db))
    if(!exact && NROW(x<-grep(table,tabs,ignore.case = T,value=T))==1)  dbListFields(db,x) else 
        if(exact) dbListFields(db,table) else if(NROW(x)>1) "No unique match" else "Not a single matching table found" 
}

table_head<-function(table="class_sessions", nrow=20,dbname=database_name){
    db<-DBI::dbConnect(RMySQL::MySQL(),user=db_user_name,password=db_password,dbname=dbname,
                       host=host_address,port=port)  # connect the DB
    on.exit(dbDisconnect(db))
    suppressWarnings(    dbReadTable(db,table)) %>% head(nrow)
}

disconnect<-function(dbhandler=1){
    dbDisconnect(dbListConnections(MySQL())[[dbhandler]])
}

insert_new_subtopic<-function(parent_topic_id='NULL', subject_id=stop("subject_id is mandatory"), topic_name=stop("new subtopic name is mandatory"),topic_info="This is added from r console"){
query_text<-sprintf("INSERT INTO topic (parent_topic_id,subject_id,classify_id,topic_name,topic_info,gi_num,gi_den,pi) VALUES(%d,%d,%d,'%s','%s',%d,%d,%d)",parent_topic_id,subject_id,1,topic_name,topic_info,0,0,0)
x<-runsql(query_text)
if(x==1) new_topic_id<-querysql(sprintf("select topic_id from topic where topic_name='%s'",topic_name))
if (nrow(new_topic_id)>1) return("you have two identical topic names, and may want to change one") else
return(new_topic_id$topic_id)    
}

show_sessions<-function(user="ALL",class="ALL", days=1,limit=30){
  state<-"NONE"
  if(all(user=="ALL",class=="ALL")) state="ALL" else
  { r<-role(user); 
  if (length(r)==1 && r==3) state="TEACHER" else
      if (length(r)==1 && r==4) state="STUDENT" else
          if (valid_class(class)) state="CLASS" else 
              return("Invalid 'user'")
  }
  script<-switch(EXPR = state,
                 "ALL"="SELECT * from class_sessions WHERE DATE(starts_on)> DATE_SUB(NOW(), INTERVAL ?da DAY) ORDER BY starts_on LIMIT ?li",
                 "TEACHER"="SELECT * from class_sessions WHERE teacher_id=?t AND DATE(starts_on)>DATE_SUB(NOW(), INTERVAL ?da DAY) ORDER BY starts_on LIMIT ?li",
                 "STUDENT"="SELECT * from class_sessions WHERE DATE(starts_on)>DATE_SUB(NOW(), INTERVAL ?da DAY)",
                 "CLASS"="SELECT * from class_sessions WHERE DATE(starts_on)>DATE_SUB(NOW(), INTERVAL ?da DAY) AND class_id =  ?ci ORDER BY starts_on LIMIT ?li",
                 "ERROR"
  )
  if(state=="STUDENT") 
    script<-sqlInterpolate(ANSI(),script,da=days)
  
  if(state=="CLASS") 
    script<-sqlInterpolate(ANSI(),script,da=days,ci=class,li=limit)
  
  if(state=="TEACHER") 
    script<-sqlInterpolate(ANSI(),script,t=user, da=days,li=limit)
  
  if(state=="ALL") 
    script<-sqlInterpolate(ANSI(),script,da=days,li=limit)
  
  if(!state =="ERROR"){
    x<-querysql(script)
     x$starts_on %<>% ymd_hms(tz = INDIA)
     x$ends_on %<>% ymd_hms(tz=INDIA)
  } 
  if(state=="STUDENT") {
    x<-x[class_id %in% classes(user = user,detail=F)]
  } else "ERROR"
  #browser()
 x[,nexts:=shift(starts_on,type = "lead")][,overlap:=ifelse(nexts<ends_on,T,F)]
 cat("\n")
 x[,-"nexts"][]
}

user_status<-function(users=monitored_users) {
  list_users(refresh = T) %>% filter(user_id %in% users) %>% select(c(1,2,4:11)) ->  output
  monitored_users<<-users
  output
}

volunteer_sessions<-function(days=100){
x<-queries(n=days)
x %>% filter(query_id %in% x$query_id,votes_received>0,allow_volunteer==1)
}

elements<-function(flip=F,detail=T,recency=24){
    b$suggo<<-refresh("b$suggo",time_gap_hours = recency)
    d$category<<-refresh("d$category",time_gap_hours = recency)
    d$elements<<-refresh("d$elements",time_gap_hours = recency)
    
    tt<-c(flip,detail)
if(identical(tt,c(F,F))) output<-
        d$category[d$elements,on=.(category_id=cat_id)][,.(count_ids=.N,cat_id=.(unique(category_id))),by=category_title]
    
if(identical(tt,c(F,T))) output<-
        d$category[d$elements,on=.(category_id=cat_id)][,.(count_rows=.N,cat_id=.(unique(category_id)),elements=.(unique(element_text))),by=category_title]

if(flip) output<-
        d$category[d$elements,on=.(category_id=cat_id)][,.(count_elements=.N,category_title=first(category_title),elements=.(unique(element_text))),by=category_id][order(-count_elements)]

output
}

category<-function(recency=24){
    d$category<<-refresh("d$category",time_gap_hours = recency)
    d$elements<<-refresh("d$elements",time_gap_hours = recency)
    d$elements[d$category,on=.(cat_id=category_id),nomatch=0][,.(count_elements=.N),by=.(category_title,cat_id)]
}
suggestions<-function(recency=24){ 
    b$suggo<<-refresh("b$suggo",time_gap_hours = recency)
    d$category<<-refresh("d$category",time_gap_hours = recency)
    d$elements<<-refresh("d$elements",time_gap_hours = recency)
    b$sessions100 <<-refresh("b$sessions100",time_gap_hours = recency)
    
    output<- d$category[b$suggo,on=.(category_id=cat_id)
               ][d$elements,on=.(category_id=cat_id),allow.cartesian=TRUE,nomatch=0
                 ][,.(sugg=first(suggestion_txt),stud=first(student_id),state=first(suggestion_state),
                      session=first(session_id),cat=.(unique(category_title))),
                   by=suggestion_id
                   ][d$entity_states, on=.(state=state_id),nomatch=0
                     ]
    cat("\n")
    output
}


#---database interaction functions----


table_all<-function(table=NULL,dbname=database_name,exact=F){
    db<-DBI::dbConnect(RMySQL::MySQL(),user=db_user_name,password=db_password,dbname=dbname,
                       host=host_address,port=port)  # connect the DB
    on.exit(dbDisconnect(db))
    if(!exact && NROW(x<-grep(table,tabs,ignore.case = T,value=T))==1)  suppressWarnings(dbReadTable(db,x)) else 
        if(exact) suppressWarnings(dbReadTable(db,table)) else if (NROW(x)>1) "No unique match" else "No match found"
}

runsql<-function(sql_text=stop("Please provide an SQL script starting with INSERT or UPDATE as first parameter and a database name as second. If database = 'jupiter_dev' you can skip"),database=database_name){
    db<-dbConnect(MySQL(),user=db_user_name,password=db_password,dbname=database,
                  host=host_address,port=port)  # connect the DB
    on.exit(dbDisconnect(db))
    rows_affected<-dbExecute(db,sql_text)
    cat(paste("\nExecuted successfully for rows:",rows_affected,"\nScript:",sql_text,"\n"))
    rows_affected
    }



writedbtable<-function(df=NULL,tablename=NULL,database=database_name,overwrite_flag=F){
  db<-DBI::dbConnect(RMySQL::MySQL(),user=db_user_name,password=db_password,dbname=database,
                     host=host_address,port=port)  # connect the DB at Amazon
  on.exit(dbDisconnect(db))
  dbWriteTable(db, tablename,df,overwrite=overwrite_flag,row.names = FALSE)
}

log<-function(api="DUMMY",user=0,parameters="DUMMY",returned_value="NOT YET IMPL"){
    #api<-deparse(sys.calls()[[sys.nframe()-1]]) - this line does not work
    write.table(data.frame(time=now(tzone=INDIA),API=api,user_id=user,Parameters=parameters,contents=as.character(returned_value)),quote=F,file=file.path(WROOT,JSON_LOG),row.names=F, col.names=F, append=T,sep=";")
}

update_event_log<-function(db,service_name='UNKNOWN', user_id=0,      UUID=0,         xml_input='XYZ',    request_time=now(),
                           xml_output='Nothing',   return_time=now()){
    sql <- "INSERT INTO event_log values  (0,?service_name, ?user_id,  ?UUID, ?xml_input, ?request_time, ?xml_output, ?return_time, 'json'  )"
    sql_new<-sqlInterpolate(ANSI(), sql, service_name=service_name, user_id=user_id,      UUID=UUID,         xml_input=xml_input,    request_time=as.character(request_time), xml_output=xml_output,   return_time=as.character(return_time))
    on.exit(dbDisconnect(db))
    x<-dbExecute(db,sql_new)
    x
}

update_state_transition<-function(type=1,id=NULL,from=NULL,to=NULL,time=as.character(NOW())){
    sql <- "INSERT INTO state_transitions VALUES (?a, ?b, ?c, ?d, ?t)"
    sql_new<-sqlInterpolate(ANSI(), sql, a=type,b=id,c=from,d=to,t=time)
    on.exit(dbDisconnect(db))
    rows_inserted<-dbExecute(db,sql_new)
    table_head("state_transitions") %>% filter(transition_time==time)
   
}

#---admin functions-----

fill_missing_time<-function(table_name=NULL,column_name=NULL){
  if(table_name=="student_index"){
    st_index %>% filter(is.na(last_updated)) %>% count(class_session_id) %>% left_join(sessions100) %>% filter(complete.cases(class_id)) ->x1
    y1<-sprintf("update student_index set last_updated=TIMESTAMP('%s') where class_session_id=%d LIMIT %d",x1$ends_on,x1$class_session_id,x1$n)
    print(paste(lapply(y1,runsql),"change(s) done for ",x1$class_session_id))
    message(sprintf("%d:total changes in DB",sum(x1$n)))
  }
  
  
}

at<-function(var=NULL) {
    if(is.data.frame(var))   
    attr(x = var,which = "last_upd") else
        message("ERROR: NO DATA FRAME IN THE VARIABLE")
}

search_functions<-function(string=str){grep(string,list_of_functions(),value=T)}

#-----declare alias or short names-----
sf<-search_functions
sq<-show_quest
ta<-table_all
ss<-show_sessions
qs<-querysql
st<-subtopics_all
mt<-main_topics
lu<-list_users
us<-user_status
sc<-start_classes
stud<-students
lv<-live_status
vs<-volunteer_sessions

#disconnect()