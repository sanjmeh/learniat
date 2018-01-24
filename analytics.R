library(tidyr)
library(data.table)
library(magrittr)
library(stringr)
library(formattable)
library(scales)
library(dplyr)
library(lubridate)
library(tibble)
library(ggplot2)
setwd("~/Dropbox/R-wd")

d$tbl_auth[,.(user_id,user_name),key="user_id"]  -> USERS
d$topic[,.(topic_id,topic_name),key="topic_id"]  -> TOPICS
d$tbl_auth[,.(user_id,school_id),key="user_id"]  -> USERSCHOOL

#pass a list of sessions to get their analysis
session_life<-function(sess=NULL,age=24){
  refresh("b$st_tr100",time_gap_hours = age)->>b$st_tr100
  b$st_tr100 %>% 
    filter(entity_id %in% sess,entity_type_id==2,to_state %in% c(1,2,5)) %>% 
    select(entity_id,to_state,transition_time) %>% 
    distinct() -> df1
  stopifnot(nrow(df1)>0)
    df1 %>% spread(key = to_state,value = transition_time) ->df2
    x1<-intersect(names(df2),c("1","2","5"))
    if('1' %in% x1) rename(df2,live_at='1') ->df3 else df3 <- df2
    if('2' %in% x1) rename(df3,open_at='2') ->df4 else  df4 <- df3
    if('5' %in% x1) {
      rename(df4, ended_at='5') -> df5 
      df6<-  mutate(df5, dur_open=trunc(time_length(interval(open_at,live_at),"minute")),
                    dur_live=trunc(time_length(interval(live_at,ended_at),"minute")))
      } else message(paste("None of the following classes have ended in the list provided:",paste(sess,collapse = ",")))
   if(exists("df6")) df6 
}

stud_scores<-function(sess=NULL,age=1000){
    refresh("b$assesm",time_gap_hours = age)->>b$assesm
    b$assesm %>% 
        filter(class_session_id %in% sess) %>% 
        group_by(class_session_id,student_id) %>% 
        summarise(ttl_ques = n_distinct(question_log_id),ttl_model=n_distinct(model_answer),avg_score=percent(mean(answer_score))) %>% 
        inner_join(USERS,by=c(student_id="user_id")) %>% 
        select(class_session_id,student_id,user_name,ttl_ques,ttl_model,avg_score)
}



school_id<-function(sess_range=5600:5700,classid=NULL,age=1000){
    refresh("b$assesm",time_gap_hours = age)->>b$assesm
    refresh("b$sindx",time_gap_hours = age)->>b$sindx
    if(is.null(classid)) 
    classid = classid(session_id = sess_range,recent=1000)
    acad = d$classes[class_id %in% classid, academic_term_id]
    scid = d$academic_terms[academic_term_id %in% acad,school_id]
    return(unique(scid))
}

stud_gi <- function(stud= 500:510,class=14,sessions=NULL,school=3){
    setkey(d$transaction_types,transaction_id)
    setkey(b$sindx,transaction_id)
  indx_long <- d$transaction_types[b$sindx[student_id %in% stud]]
  indx_map = d$school_index_map[school_id==school & index_type==1]
  setkey(indx_map,transaction_type)
  if(!is.null(class)) 
    sessions = b$sessions100$class_session_id[class==class]
  if(!is.null(sessions)) 
    indx_f = indx_long[class_session_id %in% sessions] else
        indx_f = indx_long
        indx_f[indx_map,
                 .(session=class_session_id,txid=transaction_id,stud=student_id,cnt=subtotal_of_count,score=subtotal_of_score,type=index_type,top=topic_id,wt=weight_value,time=last_updated),nomatch=0]
}

plot_gi <- function(stud=500,datewise=T,class=14){
if(datewise) stud_gi(stud=stud,class = class)[,.(stud=stud,tscore=sum(score),count=sum(cnt)),by=date(time)][,gi:=(tscore/count)] -> dt_sum
    ggplot(dt_sum,aes(x=date,y=gi,col=factor(stud)))+geom_point() +geom_smooth(method = "glm")
}

plot_activity_hist <-function(class=NULL,days=100){
    if(is.null(class)) class= classes()[[class_id]] else class=class
    setkey(b$sindx,class_session_id)
    setkey(b$sessions100,class_session_id)
    setkey(d$transaction_types,transaction_id)
    dt1 = b$sindx[b$sessions100,nomatch=0][as.double(now()-ymd_hms(starts_on)) <days]
    #browser()
    setkey(dt1,student_id)
    setkey(d$tbl_auth,user_id)
    dt2 = dt1[d$tbl_auth,nomatch=0]
    setkey(dt2,student_id,transaction_id)
    dt3 = dt2[d$transaction_types,nomatch=0,on="transaction_id"]
    ggplot(dt3[class_id %in% class],aes(x=factor(first_name),fill=factor(transaction_title))) + 
        geom_histogram(stat = "count") + 
        labs(title="Student actions", x="Student name",y="Number of actions") + 
        labs(fill="ACTION TYPE") +
        coord_flip()
}

speed <- function(max_response_time=45,class="ALL"){
  if(is.numeric(class)) stud_list <- stud(class_id  = class) else stud_list <- d$tbl_auth[role_id==4][["user_id"]]
  df1 <- b$ques_log[b$assesm,on="question_log_id"][USERS,on=.(student_id=user_id)]  %>% 
    mutate(anstime=ymd_hms(last_updated),questime=ymd_hms(start_time),quesend=ymd_hms(end_time))  %>%
    filter(answer_score>0,anstime-max_response_time<questime,student_id %in% stud_list) %>% mutate(speed=anstime-questime) %>% as.data.table()
  lev <- df1[,.(sp=mean(speed)),by="user_name"][order(sp)][["user_name"]] %>% unique()
    ggplot(df1,aes(x=factor(user_name,levels = lev),y=as.numeric(speed))) + 
    geom_boxplot(na.rm = T,outlier.colour = "red") + 
    geom_point(position = position_jitter(),alpha=0.3)
 
}



