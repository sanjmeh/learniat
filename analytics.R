library(tidyr)
library(data.table)
library(magrittr)
library(stringr)
library(formattable)
library(scales)
library(dplyr)
library(lubridate)
library(tibble)
setwd("~/Dropbox/R-wd")

d$tbl_auth[,.(user_id,user_name)]->USERS
d$topic[,.(topic_id,topic_name)]->TOPICS
d$tbl_auth[,.(user_id,school_id)]->USERSCHOOL

#pass a list of sessions to get their analysis
session_life<-function(sess=NULL){
    refresh("b$st_tr100",time_gap_hours = 24)->>b$st_tr100
b$st_tr100 %>% 
  filter(entity_id %in% sess,entity_type_id==2,to_state %in% c(1,2,5)) %>% 
  select(entity_id,to_state,transition_time) %>% 
  distinct() %>% 
  spread(key = to_state,value = transition_time) %>% 
  rename(open_at='2',live_at='1',closed_at='5') %>% 
  mutate(dur_open=trunc(time_length(interval(open_at,live_at),"minute")),dur_live=trunc(time_length(interval(live_at,closed_at),"minute")))
}

stud_record<-function(sess=NULL){
    refresh("b$assesm",time_gap_hours = 24)->>b$assesm
    b$assesm %>% 
        filter(class_session_id %in% sess) %>% 
        group_by(class_session_id,student_id) %>% 
        summarise(ttl_ques = n_distinct(question_log_id),ttl_model=n_distinct(model_answer),avg_score=percent(mean(answer_score))) %>% 
        inner_join(USERS,by=c(student_id="user_id")) %>% 
        select(class_session_id,student_id,user_name,ttl_ques,ttl_model,avg_score)
}

stud_gi<-function(sess=NULL){
    refresh("b$assesm",time_gap_hours = 24)->>b$assesm
    refresh("b$sindx",time_gap_hours = 24)->>b$sindx
    d$transaction_types[b$sindx,on=(transaction_type_id="transaction_id")]

    
    b$sindx[class_session_id %in% sess,{
                d$transaction_types[.SD,on=(transaction_type_id="transaction_id")]        
        
    },
            by=class_session_id][]
}