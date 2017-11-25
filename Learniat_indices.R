library(RMySQL)
library(ggplot2)
library(dplyr)
library(stringr)
library(lubridate)
library(formattable)
setwd("~/Dropbox/R-wd")



flag <- 0

if(flag==1) database_name="jupiter" else database_name="jupiter_dev"

if(file.exists("~/learniatDB_access")) load("~/learniatDB_access") else cat("Your DB credentials file is missing. Please save DB credentials in  '~/learniatDB_access' ")

#save(file ="learniatDB_access", host_address,db_user_name,db_password)


  



input<-10
while(!(input %in% 0:9)) {
  input<-readline(prompt="Enter 1 for AWS, 0 to abort and any other single digit for loading last saved value")
}
if( input==1) loaddata_aws()    else if ( input %in% 2:9) { 
    if(file.exists("~/learniat_variables")) load("~/learniat_variables") else cat("The DB back file is missing on your home directory. 
                                                                                  Please abort and either copy all DB tables in '~/learniat_variables' else select 1 from the promt to load the database afresh from the server")
} else stop("aborted..")
       
stopifnot(length(d)>1)  


  clean<-d
  #Open a clean copy of the DB tables and refine them.
  
  clean$class_sessions  <- within(clean$class_sessions,{
    starts_on<-ymd_hms(starts_on,tz="Asia/Kolkata")
    ends_on<-ymd_hms(ends_on,tz="Asia/Kolkata")
    period <-starts_on %--% ends_on
    duration <- format(as.period(period,unit="minutes"),digits=0)
  }
  )
  
  clean$question_log  <- within(clean$question_log,{
    start_time<-ymd_hms(start_time,tz="Asia/Kolkata")+dhours(5.5)
    freeze_time<-ymd_hms(freeze_time,tz="Asia/Kolkata")+dhours(5.5)
    end_time<-ymd_hms(end_time,tz="Asia/Kolkata")+dhours(5.5)
    ques_interval <-start_time %--% end_time
    ques_duration<-as.duration(ques_interval)
  }
  )

d$event_log$request_time %>% ymd_hms(tz="Asia/Kolkata") -> clean$event_log$request_time
d$event_log$return_time %>% ymd_hms(tz="Asia/Kolkata") -> clean$event_log$return_time
  
 
d$class_sessions %>% 
  filter(class_id %in% c(1:20)) %>% 
  group_by(class_id) %>% 
  summarise(number_sessions=n())

  #----Calculate GI topic wise
  
  clean$questions %>% 
    merge(clean$tbl_auth,by.x="teacher_id",by.y="user_id") %>% select(2,3,18) %>% 
    merge(clean$school_index_map, by.y=c("transaction_type","school_id"),by.x=c("question_type_id","school_id")) %>%
    filter(index_type==1) %>% 
    select(3,2,1,6)
  
  
  clean$answer_options %>% 
    group_by(assessment_answer_id) %>%
    summarise_at("mtc_column",NROW) %>%
    rename(student_options=mtc_column) %>%
    merge(clean$assessment_answers,by="assessment_answer_id") %>% 
    filter(answer_withdrawn>=0) %>%
    select(c(1:5,10:11)) %>%
    group_by(question_log_id) %>% 
    summarise_at(vars(answer_score,student_options,student_id),
                 funs(sum,NROW,n_distinct,mean)) %>%
    select(1,Responses=answer_score_NROW,Unique_students=student_id_n_distinct,student_options_sum,Avg_stud_selections=12,answer_score_mean) %>%
    merge(clean$question_log,by="question_log_id") %>% 
    select(c(-9,-11,-12)) %>% 
    merge(clean$questions) %>% 
    select(c(1:16)) ->x1
  

#   merge(lookup_2,df1) ->df2
#     
#   
# df2 %>%
#       group_by(topic_id) %>%
#       mutate(gi_num=weight_value*answer_score_mean*Responses,gi_den=weight_value*Responses) %>% filter(topic_id>1) %>% 
#       summarise_at(vars(gi_num,gi_den,Responses),sum) %>%
#       mutate(GI_topic=gi_num/gi_den) %>%
#       arrange(topic_id) ->GI_topicwise_SM
#     
#     GI_topicwise_SM$gi_num<-as.integer(GI_topicwise_SM$gi_num)
#     GI_topicwise_SM$gi_den<-as.integer(GI_topicwise_SM$gi_den)
#     GI_topicwise_SM$GI_topic<-percent(GI_topicwise_SM$GI_topic,1)
    
    # print(GI_topicwise_SM)
    


    #calcaulate summaries    

    
    merge(clean$student_index, clean$tbl_auth,by.x="student_id",by.y="user_id") %>%
      merge(clean$school_index_map, by.x=c("transaction_id","school_id"),by.y=c("transaction_type","school_id")) %>% 
      filter(index_type==1) %>%
      group_by(student_id,topic_id,class_session_id,transaction_id,weight_value) %>% 
      summarise_at(vars(subtotal_of_count,subtotal_of_score),funs(sum,mean)) %>% head
      filter(transaction_id<5) %>% head(30)
    
      
    
    merge(clean$student_index, clean$tbl_auth,by.x="student_id",by.y="user_id") %>%
      merge(clean$school_index_map, by.x=c("transaction_id","school_id"),by.y=c("transaction_type","school_id")) %>%
      filter(index_type>0) %>%
      group_by(student_id,class_session_id,transaction_id) %>% 
      summarise_at(vars(subtotal_of_count,subtotal_of_score),funs(sum,mean)) %>% head
    
    
    merge(clean$student_index, clean$tbl_auth,by.x="student_id",by.y="user_id") %>%
      merge(clean$school_index_map, by.x=c("transaction_id","school_id"),by.y=c("transaction_type","school_id")) %>%
      filter(index_type==1 & topic_id==45) %>% select(1:3,5:8,13,27:28) %>% 
      group_by(topic_id,weight_value) %>% 
      summarise_at(vars(subtotal_of_count,subtotal_of_score),funs(sum,mean)) %>% head(20)
      select(1:2,count=subtotal_of_count_sum,score=subtotal_of_score_sum,2) %>% 
      mutate(num=digits(weight_value*score,3),den=weight_value*count,GI_top=percent(num/den,2)) %>%
      summarise_at(vars(num,den,count),sum) %>% 
      mutate(GI=num/den) %>% 
      head(20)
      
      
    merge(clean$student_index, clean$tbl_auth,by.x="student_id",by.y="user_id") %>% 
        group_by(school_id,transaction_id) %>% 
        tally %>% 
        merge(clean$transaction_types) %>% 
        arrange(school_id,transaction_id) -> TX_SUMM
  
  
      
      #----- upto  here
        
 
    #set up new variables (outside the list d) as transformed tables.
    acad_terms<- within(d$academic_terms,{
      start_date<-ymd(start_date,tz="Asia/Kolkata")+dhours(5.5)
      end_date<-ymd(end_date,tz="Asia/Kolkata")+dhours(5.5)
      term <-start_date %--% end_date
      school_id<-factor(school_id,levels = d$schools$school_id, labels = d$schools$full_name)
    }
    )
    
    
    ass_ans<- within(d$assessment_answers,{
      answer_score<-percent(answer_score,digits = 0)
    }
    )
    
    classes<- within(d$classes,{
      teacher_id<-factor(teacher_id,levels = d$tbl_auth$user_id[d$tbl_auth$role_id==3],
                         labels = d$tbl_auth$user_name[d$tbl_auth$role_id==3])
    }
    )
    
    topics<- within(d$topic,{
      
      #  topic_id<-factor(topic_id,levels = topic_id,
      #                         labels = paste(topic_name,"(",str_c(topic_id),")"))
      #commented above lines as it prevents merging on topic_id
      parent_topic_id<-factor(parent_topic_id,levels = topic_id,
                              labels = paste(topic_name,"(",str_c(topic_id),")"))
      subject_id<-factor(subject_id,levels=d$subjects$subject_id,
                         labels = paste(d$subjects$subject_name,"(",str_c(d$subjects$subject_id),")"))
      suppressWarnings(  last_updated<-ymd_hms(last_updated,tz="Asia/Kolkata")+dhours(5.5) )
    }
    )
    
    questions<- within(d$questions,{
      question_type_id<-factor(question_type_id,levels = d$question_types$question_type_id,
                               labels = d$question_types$question_type_title)
      # topic_id<-factor(topic_id,levels = topics$topic_id,
      #                 labels = paste(topics$topic_name,"(",str_c(topics$topic_id),")"))
      teacher_id<-factor(teacher_id,levels = d$tbl_auth$user_id[d$tbl_auth$role_id==3],
                         labels = d$tbl_auth$user_name[d$tbl_auth$role_id==3])
    }
    )
    
    #merge and create an expanded question_log table
    merge(ques_log,questions,by = "question_id") %>%
      merge(topics,by="topic_id") %>%
      merge(sessions,by="class_session_id") %>%
      merge(ass_ans,by="question_log_id") -> ques_log_exp
    
    ques_log_exp<-within (ques_log_exp,{
      topic_id<-factor(topic_id,levels = topics$topic_id)
    }
    )
    
    
    
    not_needed_columns<-c("last_updated","last_updated.y","last_updated.x","teacher_scribble_id","scribble_id","classify_id",
                          "classification","topic_info","total_stud_registered",
                          "class_session_id.y")
    
    ques_log_exp<-ques_log_exp[,!colnames(ques_log_exp) %in% not_needed_columns]
    
    
    
   # plot the distribution of questions
    dquestions %>%
      mutate(topic=factor(topic_id,levels=topics$topic_id,
                          labels = paste(topics$topic_name,"(",topics$topic_id,")","[",topics$parent_topic_id,"]"))) %>%
      filter(topic_id %in% 101:150) %>%
      ggplot(aes(question_type_id)) + geom_bar(aes(fill=question_type_id)) + facet_wrap(~topic, nrow =4) +
      labs(title = "Questions in Learniat - split up on topics",subtitle="topic id 101 to 200")



  
  
  # ans_opt<- within(d$answer_options,{
  #   suppressWarnings(  updated_on<-ymd_hms(updated_on,tz="Asia/Kolkata")+dhours(5.5))
  #   mtc_column<-factor(mtc_column,levels = c(1L,2L))
  #   old_sequence<-factor(old_sequence,levels = 1:10)
  #   mtc_sequence<-factor(mtc_sequence,levels = 1:10)
  # }
  # )
  
  
  




#--------
# class_sessions$starts_on<-ymd_hms(class_sessions$starts_on, tz="Asia/Kolkata")+dhours(5.5)
# class_sessions$ends_on<-ymd_hms(class_sessions$ends_on, tz="Asia/Kolkata")+dhours(5.5)
# state_transitions$transition_time<-ymd_hms(state_transitions$transition_time, tz="Asia/Kolkata")+dhours(5.5)
# 
# cs_merged1<-merge(class_sessions,classes,by="class_id")
# cs_exp<-mutate(cs_merged1,
#                period=starts_on %--% ends_on, 
#                duration=as.duration(period),
#                class_name=factor(class_name, 
#                                  levels = classes$class_id,
#                                  labels = paste(classes$class_name,classes$academic_term_id)))
# 
# new_starts_on<-cs_exp[cs_exp$period<0,]$starts_on+as.integer(cs_exp[cs_exp$period<0,]$duration-3600)``
# session_ids_to_be_changed<-cs_exp[cs_exp$period<0,]$class_session_id
# #rows_to_be_changed<-row.names(  cs_exp[cs_exp$period<0,])
# 
# names(session_ids_to_be_changed)<-1:length(session_ids_to_be_changed)
# for (i in 1:NROW(session_ids_to_be_changed)){
# dbGetQuery(db,paste("update class_sessions set starts_on=",str_c(new_starts_on[i]), "where class_session_id =",str_c(unname(session_ids_to_be_changed[i]))))
# }
# 
# stud_index$Last_Updated<-ymd_hms(stud_index$Last_Updated,tz="Asia/Kolkata")+dhours(5.5)
# stud_class$last_updated<- ymd_hms(stud_class$last_updated,tz="Asia/Kolkata")+dhours(5.5)
# topic_log$transition_time<- ymd_hms(topic_log$transition_time,tz="Asia/Kolkata")+dhours(5.5)
# transitions$transition_time<- ymd_hms(transitions$transition_time,tz="Asia/Kolkata")+dhours(5.5)
# q_log$start_time<- ymd_hms(q_log$start_time,tz="Asia/Kolkata")+dhours(5.5)
# q_log$freeze_time<- ymd_hms(q_log$freeze_time,tz="Asia/Kolkata")+dhours(5.5)
# q_log$end_time<- ymd_hms(q_log$end_time,tz="Asia/Kolkata")+dhours(5.5)
# q_log$average_score<-percent(q_log$average_score,digits=2)
# sessions$ends_on<- ymd_hms(sessions$ends_on,tz="Asia/Kolkata")+dhours(5.5)
# sessions$starts_on<- ymd_hms(sessions$starts_on,tz="Asia/Kolkata")+dhours(5.5)
# stud_time$stud_time<- as.duration(stud_time$stud_time)
# 
# 
# state<-factor(1:11,levels=states$state_id,labels = states$state_description)
# index<-factor(1:2,levels = index_type$index_id,labels = index_type$index_name)
# cl_short<-classes[,c(1,2)]
# sessions2<-merge(sessions,cl_short)
# sessions2$class_id<-factor(sessions2$class_id,labels = sessions2$class_name)
# sessions2$class_name<-NULL
# 
# 
# 
# t2<-mutate(topic,gi=percent(gi_num/gi_den,digits=2))[,-c(6:7)]
# 
# 
# ques_master<-merge(q_log,questions)
# ques_master2<-mutate(ques_master,ym<-factor(paste0(year(start_time),"-",formatC(month(start_time),width=2,flag="0"))))
# 
# ques_1<-ques_master2[,c(1,2,4,8:12,16,18)]
# colnames(ques_1)[9]<-"Month"
# 
# 
# ques_1$question_type_id<-factor(ques_1$question_type_id,levels = c(1:6), labels = ques_type$question_type_title[c(1:6)])
# ques_1$teacher_id<-factor(ques_1$teacher_id,levels = users$user_id[users$role_id==3],labels = users$user_name[users$role_id==3])
# ques_1$on_the_fly<-factor(ques_1$on_the_fly,levels = c(1,0),labels = c('ON THE FLY','PREPARED'))
# 
# head(ques_1[(ques_1$answer_count>0 & ques_1$average_score!=0),][!is.na(ques_1$answer_count),],400) ->ques_2
#-----


save.image(file="learniat_variables")


  #test with original database doesnt give this warning
    # Warning messages:
    #   1: In unclass(e1) + unclass(e2) :
    #   longer object length is not a multiple of shorter object length
    # 2: In format.data.frame(x, digits = digits, na.encode = FALSE) :
    #   corrupt data frame: columns will be truncated or padded with NAs
    
      d$questions %>% 
      merge(d$tbl_auth,by.x="teacher_id",by.y="user_id") %>% select(2,3,18) %>% 
      merge(d$school_index_map, by.y=c("transaction_type","school_id"),by.x=c("question_type_id","school_id")) %>%
      filter(index_type==1) %>%
      select(3,2,1,6) -> lookup_2
    
    
    b$ans_opt %>% 
      group_by(assessment_answer_id) %>%
      summarise_at("mtc_column",NROW) %>%
      rename(student_options=mtc_column) %>%
      merge(d$assessment_answers,by="assessment_answer_id") %>% 
      filter(answer_withdrawn>=0) %>%
      select(c(1:5,10:11)) %>%
      group_by(question_log_id) %>% 
      summarise_at(vars(answer_score,student_options,student_id),
                   funs(sum,NROW,n_distinct,mean)) %>%
      select(1,Responses=answer_score_NROW,Unique_students=student_id_n_distinct,student_options_sum,Avg_stud_selections=12,answer_score_mean) %>%
      merge(d$question_log,by="question_log_id") %>% 
      select(c(-9,-11,-12)) %>% 
      merge(d$questions) %>% 
      select(c(1:16))

