library(dplyr)
library(tidyr)
library(data.table)
library(lubridate)
library(stringr)
library(xml2)
library(jsonlite)
library(googlesheets)
source("~/Dropbox/R-wd/quick.R")

loaded_file <- parent.frame(2)$ofile
India="Asia/Kolkata"
if(str_detect(loaded_file,"Dropbox")) setwd("~/Dropbox/R-wd")
root_jabber <- "http://52.76.85.25"
root_api <- "http://54.251.104.13/logs"

robust_read_xml <- function(input_string) {
   tryCatch(read_xml(input_string),error=function(e){
     return(0)
   })
}

#list all google sheet titles, filtered by name and author.
glist<-function(name="ALL",athr="ANY", refresh=F){
    if(refresh) mysheets<<-gs_ls()
    if(athr!="ANY" && name!="ALL") mysheets %>%  
            filter(grepl(name, sheet_title,ignore.case = T), grepl(athr, author,ignore.case = T)) %>% 
            select(1:2,5) else
                if(athr=="ANY" && name!="ALL") mysheets %>%  
            filter(grepl(name, sheet_title,ignore.case = T)) %>% 
            select(1:2,5) else
                if(athr!="ANY" && name=="ALL") mysheets %>%  
            filter(grepl(athr, author,ignore.case = T)) %>% 
            select(1:2,5) else
                if(athr=="ANY" && name=="ANY") mysheets %>%  select(1:2,5)
}

json_logs<-function(last_update_minutes=60,weblogs=T){
    file<- ifelse(weblogs, file.path(ROOT,JSON_LOG),file.path(WROOT,JSON_LOG))
if(file.exists("json.logs") && file.info("json.logs")$mtime>now()-dminutes(last_update_minutes)) file="json.logs"  
x<-fread(input = file,sep = ";",fill = T,
         col.names = c("time","service_name","user_id","input_string","contents"),
         colClasses = c("Date","character","character", "character","character"), stringsAsFactors = F)
x$time<-ymd_hms(x$time,tz = India)
if(grepl("http",file)) fwrite(x,file = "json.logs",sep = ";")
x[order(-time)]
}

download_log<-function(file_name="mysql.log",jabber=F){
    full_path<-file.path(ifelse(jabber,root_jabber,root_api),file_name)
    readLines(full_path) 
}

api_flow<-function(logdt=NULL){
    logdt %>% 
        group_by(Minute=sprintf("%02d%3s:%02d:%02d",day(time),lubridate::month(time,label = T),lubridate::hour(time), lubridate::minute(time)),User=as.character(user_id),type) %>% 
        count(service_name) %>% 
        arrange(Minute) %>% as.data.table()
}


sql_flow<- function(logdt=NULL){
    logdt %>% 
        group_by(Minute=sprintf("%02d%3s:%02d:%02d",day(dt),lubridate::month(dt,label = T),lubridate::hour(dt), lubridate::minute(dt)),service_name) %>% 
        count(service_name) %>% 
        arrange(Minute) %>% as.data.table()
}

api_log<-function(start_mdh=101112,end_mdh=101113,json_recency=5){
    {
        time_range<-interval(start = ymd_h(paste0('2018',start_mdh),tz = India),end =ymd_h(paste0('2018',end_mdh),tz = India))
        start_time <- ymd_hms(paste0('2018',start_mdh,'0000'),tz=India)
        end_time<-    ymd_hms(paste0('2018',end_mdh,'0000'),tz = India)
    }
    x<- api_archive(start = start_mdh,end=end_mdh)
    if(is.data.table(x) && nrow(x) > 0)
     x[,API:="XML"] else  return ("NO XML API ROWS RETURNED") 
    
    y<-json_logs(last_update = json_recency)
    if(is.data.table(y) && nrow(y) > 0)
        y[,API:="JSON"] else  return ("NO JSON API ROWS RETURNED") 
    
      y1<- y[int_overlaps(time_range,time %--% time)]
    bind_rows(y1,x)[order(time)]
}

api_archive<-function(start='101315',end='101316',number_rows=2000, truncated=F,xml_cols=T,trunc_xml=T){
    treat_error<-function(df,i,output=T){
        if(output) {
            df$type[i]<-"INVALID xml_output"
            df$contents[i]<-"Errored XML"
        } else {
            df$type[i]<-"INVALID xml_input"
            df$input_string[i]<-"Errored XML"
        }
        cat(paste0("[",df$id[i],"]"))
    }
    gotolastnode<-function(node){
        while(xml_length(node)[1]>0)
            node<-xml_children(node)[1]
        node
    }
    start_time <- ymd_hms(paste0('2018',start,'0000'),tz=India)
    end_time<-    ymd_hms(paste0('2018',end,'0000'),tz = India)
    t1<-proc.time()
    string1<-sprintf("select * from event_log where request_time BETWEEN '%s' AND '%s' limit %d",start_time,end_time,number_rows)
    top_rows<-suppressWarnings(querysql(string1,database = database_name)) %>% rename(id=event_log_id) %>% 
               mutate(request_time=ymd_hms(request_time,tz = India),return_time=ymd_hms(return_time,tz = India)) %>% arrange(request_time) %>% as.data.table()
    t2<-proc.time()
    message("Time taken to run the SQL script:",(t2-t1)[3])
    if (nrow(top_rows)==0) return ("ZERO API ROWS FOUND")
    top_rows$user_id<-as.character(top_rows$user_id)
      message("extracting XML APIs from event logs ...")
        for(i in 1:nrow(top_rows)){
            exit<-F
            #process xml_input
            if(top_rows[i,"service_name"]=="Login") next # Skip Login as we get its details in jsonlogs
            xnode<-robust_read_xml(top_rows$xml_input[i]) # function that does not stop processing if not a valid xml
            if(is.numeric(xnode)){ 
                treat_error(df = top_rows,i = i,output = F)
            } else
            {
                service_node<-xml_find_all(x = xnode,xpath = "Action/Service")
                action_node<-xml_find_all(x = xnode,xpath = "Action")
                action_len<-xml_length(action_node)
                if(action_len > 0)  {
                    top_rows$service_name_extract[i] <- xml_text(service_node)
                    parameter_names<-xml_name(xml_siblings(service_node))
                    parameter_values<-xml_siblings(service_node) %>% xml_contents
                    param_string<- paste0(parameter_names,":",parameter_values,collapse = "; ")
                    top_rows$input_string[i]<-param_string
                    top_rows$input_len[i]<-action_len
                } else
                        treat_error(df = top_rows,i,output = T)
            } 
                cat(paste0(i,","))
            
            #--- commented-----
            #status_node<-xml_find_first(action_node,"Status")
            # while (xml_length(xnode)==1) xnode<-xml_children(xnode)
            # size_input<-xml_length(xnode)
            # top_rows[i,"input_nodes"] <- size_input
            # if(size_input > 1){
            #   final_node<-xml_children(xnode)
            #   top_rows[i,"Service_Name"] <-xml_text(final_node[1])
            # #  if(grepl("broadcast",top_rows[i,"Service_Name"],ignore.case = T)) browser()
            #   top_rows[i,"Input"]        <-xml_text(final_node[2])
            #   top_rows[i,"tagname"]     <-  xml_name(final_node[2])
            #   top_rows[i,"input_string"]  <- paste0(xml_name(final_node),":",xml_contents(final_node),collapse = "; ")
            #--------
            
            #process xml_output (the same way as xml_input)
            xnode<-robust_read_xml(top_rows$xml_output[i])
            if(is.numeric(xnode)){
              treat_error(df = top_rows,i)
              next
            } else
            {
                action_node<-xml_find_all(x = xnode,xpath = "/Root/SunStone/Action")
                action_len<-xml_length(action_node)
                if(action_len==0)  {
                    treat_error(df = top_rows,i); 
                    next
                }
            status_node<-xml_find_first(action_node,"Status")
            status_value <- xml_contents(status_node)
            if(action_len>1) {
                sib_node <- status_node %>% xml_siblings
                last_node<-gotolastnode(sib_node)
                node_names<-  last_node %>% xml_name
                node_values<- last_node %>% xml_contents()
                node_string<- paste0(node_names,":",node_values,collapse = "; ")
            } else
                node_string<-"Missing sibling xml nodes"
            top_rows[i,"status"] <- xml_text(status_value)
            top_rows[i,"contents"] <- node_string
            top_rows[i,"output_dim1"]<-action_len
            top_rows[i,"output_dim2"]<-xml_length(xml_siblings(status_node)[1])
              
               #if output contents are to be trucated
            #   if(trunc_xml && length(unique(xml_name(xniece)))==1) {
            #     cat("!")
            #     top_rows[i,"contents"] <- paste(xml_contents(xml_children(xniece[1])),collapse = "; ")
            #   } else
            #     top_rows[i,"contents"] <- paste(xml_contents(xml_children(xniece)),collapse = "; ")
            # } else
            # {
            #   top_rows[i,"output_dim1"]<- length(xsib)
            #   top_rows[i,"output_dim2"]<- 0
              
            }
        }
  print(":",quote = F)
    # if(truncated) 
  top_rows[,time:=request_time
           ][,response_delay:=difftime(return_time,request_time)
             ][,-c("request_time","return_time","type","service_name_extract","UUID")
               ][order(time,id)]
    #     top_rows %<>% select(event_log_id,service_name,user_id,request_time) else
    # if(!xml_cols) top_rows %<>% select(-starts_with("xml"),-UUID) else
    #     top_rows %<>% select(-UUID)
    # top_rows %>% as.data.table
  }

badly_formed_xml<-function(start='101315',end='101316',number_rows=100000){
  start_time <- ymd_hms(paste0('2018',start,'0000'),tz=India)
  end_time<-    ymd_hms(paste0('2018',end,'0000'),tz = India)
  string1<-sprintf("select * from event_log where request_time BETWEEN '%s' AND '%s' limit %d",start_time,end_time,number_rows)
  all_rows<-suppressWarnings(querysql(string1,database = database_name)) %>% 
    mutate(request_time=ymd_hms(request_time,tz=India)) %>% select(service_name,type,request_time,xml_input,xml_output) %>%  as.data.table()
  for(i in 1:nrow(all_rows)){
    if(all_rows[i,"service_name"]=="Login") next
    xnode<-robust_read_xml(all_rows$xml_input[i])
    if(is.numeric(xnode)) print(sprintf("Input  XML %s(%d)  badly formed on %s:%s",all_rows$service_name[i],i, all_rows$request_time[i], all_rows$xml_input[i]))
    
    xnode<-robust_read_xml(all_rows$xml_output[i])
    if(is.numeric(xnode)) print(sprintf("Ouput  XML %s(%d) badly formed on %s:%s",all_rows$service_name[i],i, all_rows$request_time[i], all_rows$xml_output[i]))
  }
}

extr_collab<-function(pattern="FetchCategory", pattern_in="xml_input",start=1101,end=1231,limit=1000){
  dt1 <-    ymd(paste0('2018',start),tz=India)
  dt2 <-    ymd(paste0('2018',end),tz = India)
  sql1<-sprintf("select request_time,xml_input,xml_output from event_log where request_time BETWEEN '%s' AND '%s' limit %d",dt1,dt2,limit)
  dump<-as.data.table(querysql(sql1))
  dump[grep(pattern,get(pattern_in))]
}

jout <- function(dt=NULL,row=NULL,column=""){
    get("dt")$contents[row] %>% jsonlite::prettify()
}

fetch_xml<-function(log_id=87788,ejabberd=T,output=T,lvl=1,name=F){
  if(ejabberd) {
    row_of_interest<-suppressWarnings(querysql(sprintf("select * from archive where id=%d",log_id),database = "ejabberd_1609") )
    if(nchar(row_of_interest$txt)<10) x<- row_of_interest$xml %>% robust_read_xml else
        x <- row_of_interest$txt %>% robust_read_xml
  } else
  {
    row_of_interest<-suppressWarnings(querysql(sprintf("select * from event_log where event_log_id=%d",log_id)))
    if(output) x <- row_of_interest$xml_output %>% robust_read_xml else x<- row_of_interest$xml_input %>% robust_read_xml
  }
        
  if(is.numeric(x) & !ejabberd) {
        y<- ifelse(output,row_of_interest$xml_output,row_of_interest$xml_input)
    message("ERROR IN XML")
  } else
    if(!name) y <- switch(lvl,
                x %>% xml_contents,
                x %>% xml_contents %>% xml_children,
                x %>% xml_contents %>% xml_children %>% xml_children,
                x %>% xml_contents %>% xml_children %>% xml_children %>% xml_children,
                x %>% xml_contents %>% xml_children %>% xml_children %>% xml_children %>% xml_children,
                x %>% xml_contents %>% xml_children %>% xml_children %>% xml_children %>% xml_children %>% xml_children,
                x %>% xml_children %>% xml_contents %>% xml_children %>% xml_contents,
                x %>% xml_children %>% xml_children %>% xml_children %>% xml_children %>% xml_children %>% xml_contents,
                x %>% xml_children %>% xml_children %>% xml_contents,
                "Error"
    ) else
  y <- switch(lvl,
              x %>% xml_children,
              x %>% xml_children %>% xml_children,
              x %>% xml_children %>% xml_children %>% xml_children %>% xml_name,
              x %>% xml_children %>% xml_children %>% xml_children %>% xml_children %>% xml_name,
              x %>% xml_children %>% xml_children %>% xml_children %>% xml_children %>% xml_children %>% xml_name,
              "Error"
  ) 
  return(y)
}

xmpp_coding<-function(sheet_name="master"){
        df<-gs_read(gs_title(x = "XMPP coding"),col_names=F, ws = sheet_name,range = cell_limits(ul = c(2,1),lr = c(NA,1)))
        df %<>%  rename(ios=X1)
        df %<>%  mutate(ios_nospace=gsub("\\s","",ios))
        df %<>% mutate(clean=gsub("letk(\\w+)=.(\\d+).*","\\1 \\2",ios_nospace)) 
        #df %<>% mutate(label=gsub("([A-Za-z]+) (\\d+)","\\1",clean),code=(gsub("([A-Za-z]+) (\\d+)","\\2",clean)))
        dt<-str_split(df$clean,pattern = " ",simplify = T) %>% as.data.table()
        #df2 %<>% filter(complete.cases(code))
        dt<-dt[V2>0,.(code=as.numeric(V2),label=V1)]
        setkey(dt,code)
        dt<- unique(dt)
    update_time(dt)
}

xmpp_new<-function(mdh1=0,mdh2=0){
    #----- function to process xml_node into 
    proc_xml<-function(xnode){
        if (length(xml_name(xml_child(xnode)))==1) {
            type=xml_name(xml_child(xnode))
            text=xml_text(xml_child(xnode))
        } else
        if (length(xml_name(xml_child(xnode)))>1)
        {
            type="Message"
            names<-xml_name(xnode)
            values<-xml_text(xnode)
            array<-paste(names,values,sep = ":")
            text<-paste(array,collapse = ";")
        } 
        text
    }
#---- start of xmpp_new-----
        start<-suppressWarnings(ymd_h(paste0("2018",mdh1),tz = India))
        end<-  suppressWarnings(ymd_h(paste0("2018",mdh2),tz = India))
    raw_data<-suppressWarnings({querysql(sprintf("select * from archive where created_at BETWEEN '%s' AND '%s'",start, end),database = "ejabberd_1609")})
    if(nrow(raw_data)>0){
        created_at<-ymd_hms(raw_data$created_at,tz = India)
        raw_xml_text<-raw_data$xml
        kind<-raw_data$kind
        from<-str_extract(raw_data$bare_peer,"\\d+")
        to<-str_extract(raw_data$username,"[^@]+")
        id<-raw_data$id
        xnodes<-lapply(raw_data$xml,read_xml)
        type_list<-sapply(raw_data$txt,function(x) {
            if(x!="") {
                x1<-read_xml(x)
                t<-xml_find_all(x1,xpath = "Type") %>% xml_text %>% as.numeric
                return(t)
            } else return(NA)
        }
        )
        outp<- data.table(id, created_at,from,to,kind,code=type_list,body=sapply(xnodes,proc_xml),stringsAsFactors = F) %>% merge(y = xmpp_code_lookup,by = "code",all.x = T) %>% .[order(created_at)]
    } else 
        outp<- "No revcords found in XMPP archive table in the time range"
    outp
}

if(!exists("xmpp_code_lookup") || is.null(at(xmpp_code_lookup))) xmpp_code_lookup <-xmpp_coding()

# this function has errors; please use xmpp_new()
xmpp_archive<-function(mdh1=0,mdh2=0,width=80,limit=2000,lookup=xmpp_code_lookup,refresh=F,raw=F,serial=NULL){
  start<-ymd_h(paste0("2018",mdh1),tz = India)
  end<-  ymd_h(paste0("2018",mdh2),tz = India)
      if(refresh) {
        sql_text<-sprintf("select * from archive where created_at between '%s' AND '%s' order by created_at desc",start,end)
        suppressWarnings(querysql(sql_text,database = "ejabberd_1609")) %>% mutate(created_at=ymd_hms(created_at,tz = India)) %>% as.data.table ->> xmpp_sql_dump
    }
        if(exists("xmpp_sql_dump")) dump_df<-as.data.table(xmpp_sql_dump) else return("Error: 'xmpp_sql_dump' does not exist - use refresh=T option")
        range<-interval(start,end)
        top_rows<- dump_df[int_overlaps(created_at %--% created_at,range)]
        if(nrow(top_rows)==0) return("Error : Zero rows returned; check the range of dates")
        top_rows$from<-as.numeric()
        top_rows$to<-as.numeric()
        top_rows$type<-as.numeric()
        top_rows$body<-as.character()
        ktopics<-c("topic","room_id","is_state_changed","session_state")
        kquestions<-c("room_id","question")
        for(i in 1:nrow(top_rows)){
            x1<-robust_read_xml(top_rows$txt[i])
            if(is.numeric(x1)){
              top_rows$type[i]<-0
              x2<-robust_read_xml(top_rows$xml[i])
              if(!is.numeric(x2) && names(xml_attrs(x2))[1]=="to"){
                xj<-fromJSON(xml_text(xml_children(x2)))
                if(identical(names(xj),ktopics))
                  top_rows$body[i]<-paste0("topic_id:",xj$topic$topic_id,"; State:",
                             xj$topic$topic_state,"; is_state_changed:", xj$is_state_changed,"; Session_state:",xj$session_state) else
                               if(identical(names(xj),kquestions))
                                 top_rows$body[i]<-paste0("ques_id:",xj$question$question_id,"; ques_type:",xj$question$question_type,"; ques_state:",xj$question$question_state)
                tmp<-xml_attrs(x2)[1]
                tmp<-gsub("(\\w+)@(.*)","\\1",tmp)  
              top_rows$to[i]<-tmp
              top_rows$from[i]<-xml_attrs(x2)[2]
              } 
              next
            }
            parents<-xml_name(xml_children(x1))
            if(length(parents)>4) 
                {message(paste("Error: More than 4 elements detected in XML row :",i))
                message("Skipping the row...")
            }
            xType=xml_find_all(x1,xpath="Type")
            type=xml_contents(xType)
            
            xFrom=xml_find_all(x1,xpath="From")
            from=xml_contents(xFrom)
            
            xTo=xml_find_all(x1,xpath="To")
            to=xml_contents(xTo)
            tmp<-xml_text(to)
            tmp<-gsub("(\\w+)@(.*)","\\1",tmp)
            top_rows[i,"from"] <-xml_text(from)
            top_rows[i,"to"]   <-tmp
            top_rows[i,"type"] <-as.numeric(xml_text(type))
            if(length(parents)==4 && parents[4]=="Body"){
              xBody=xml_find_all(x1,xpath="Body")
              xnode<-xBody
              while(sum(xml_length(xnode))>0) xnode<-xml_children(xnode)
                keys=xml_name(xnode)
                values=xml_text(xnode)
                top_rows[i,"body"] <-paste0(keys,":",values,collapse = "; ")
            }
        }
        top_rows<- top_rows[xmpp_code_lookup,on=.(type=code)]
        if(is.null(serial)) {
          if(raw)
          {
            setkey(top_rows,id)
            unique(top_rows)
          }
            #   top_rows %>% 
            # select(id,created_at,from,to,type,label,body,txt,xml) %>% distinct() %>% arrange(id) %>% as.data.table() else
        } else 
            top_rows[serial,"xml"]
}

# xmpp_xml<-function(date=NULL,user=NULL,rows=5000,max_width=5000){
#   x<-xmpp_archive(rows,max_width)
#   if(is.null(user)) x %>% filter(created_at==date) ->result else  x %>% filter(created_at==date,username==user) ->result
#   x<-xmlTreeParse(result$txt)
#   x$doc$children$Message
# }


php_log<-function(path="NULL",file="xx.txt"){
    # if (date<10) date=paste0("0",date)
    # if (month<10) month=paste0("0",month)
    #path1<-paste0("http://54.251.104.13/learniat_",app)
    #path2<-paste0("/application/logs/log-2018-", month)
    #paste0(path1,path2,"-",date,".log")
    
    url_string<-file.path(root_api,file)
  con_log<-url(url_string)
  x<-readLines(con_log)
  close(con_log)
  data.table(x)
}

ip_log<-function(no_hits=200,ip=NULL){
  read.csv("http://54.251.104.13/logs/access.log",
           sep = " ",stringsAsFactors = F,header = F) ->dump
  if(is.null(ip)) dump %>% 
    mutate(date=dmy(str_sub(V4,2,10)),timestamp=dmy_hms(str_sub(V4,2)),hour=hour(timestamp)) %>% 
    group_by(date,hour,V1) %>% 
    summarise(hits=n()) %>% 
    filter(hits>no_hits) %>% 
    arrange(date,hour) else
      dump %>% 
    mutate(date=dmy(str_sub(V4,2,10)),timestamp=dmy_hms(str_sub(V4,2))) %>% 
    filter(V1==ip) %>% select(timestamp,IPaddr=V1, desc=V6) %>% arrange(timestamp)
}


dumplog<-function(file=MYSQL_LOG){
    file=file.path(ROOT,file)
    dt_dumped_file<-as.data.table(readLines(file))
    dt_dumped_file$V1
}

clean_log<-function(dump=NULL,clean_file="clean_dump.txt",mysql=T){
    replace_faulty_bunch<-function(logdump=NULL,list_def){
        for (i in 1:length(list_def)){
            rowspasted<-paste(logdump[list_def[[i]]],collapse  = " ")
            one_earlier<-logdump[list_def[[i]][1]-1]
            rowspasted<-paste(one_earlier,rowspasted)
            logdump[list_def[[i]][1]-1]<-  gsub(pattern = "\\s+",replacement = " ",x = rowspasted)
        }
        logdump
    }
    remove_junk<-function(dump,onebunch){
        if(length(onebunch)>1) dump[-onebunch[2:length(onebunch)]] else
            dump[-onebunch]
    }  
    group_fr<-function(frows){
        group<-list()
        start=1; end=NULL; k<-1; N<-length(frows)
        if(N>0) for(i in 1:length(frows)){
            if(i==length(frows) || frows[i+1]-frows[i]!=1) {
                end<-i
                group[[k]]<-frows[start:end]
                start<-i+1;end=NULL;k<-k+1
            }
        } else return()
        group
    }
    if(mysql) dump<-dump[4:length(dump)]
    fr<- grep("^2018",dump,invert = T)
    if(length(fr)>0){ 
        message(sprintf("Identified %d defective log rows NOT starting with '2018'.. trying to identify bunching",length(fr)))
        message(paste(fr,collapse = ","))
        list_def = group_fr(frows = fr)
        message(sprintf("\nIdentified %d bunches of rows .. trying to fix by wrapping...",length(list_def)))
        message(str_c(list_def,collapse = ";"))
        dump2<-replace_faulty_bunch(logdump = dump,list_def = list_def)
        dump_modified<-dump2
        for(i in 1:length(list_def)){
            dump_modified<-remove_junk(dump_modified,list_def[[i]])
        }    
        message(sprintf("\nRaw dump had %d rows, and the clean dump has %d rows returned. All successful.",length(dump),length(dump_modified)))
    } else dump_modified<-dump
    cat("\n------\n")
    if(mysql)
        dump_tabs <- gsub("Init DB","Initdb",dump_modified) %>% 
        gsub("(Z[\t ]+)(\\d+) ([QCI][a-z]+)([\t ])",replacement = "Z\t\\2\t\\3\t",x = .) else
            dump_tabs <-  dump_modified
    
    valid_rows<-grep("^2018",dump_tabs,value = T)
    dt_onevar<-as.data.table(valid_rows)
    dt_no_event_log <- dt_onevar[!grepl("event_log",valid_rows)]
    browser()
    #print(setdiff(valid_rows,dump))
    data.table::fwrite(x = dt_no_event_log,file = clean_file,sep = "\t",quote = F)
}


check_range<-function(clean_text_file=NULL,df=NULL,time_stamp_column="time",type_column="type"){
  if(!is.null(clean_text_file)) {
      x<-fread(clean_text_file,skip = 1)[,1]
      dates<-as.POSIXct(gsub("^(2018-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:\\d+).*","\\1 \\2", x$V1),tz = "UTC")
      attributes(dates)$tzone<-India
  r<-range(dates)
  } else
 if(!is.null(df)) {
     DT<-as.data.table(df)[order(get(time_stamp_column))]
     r<-DT[,.(date_range=.(range(get(time_stamp_column))), tot_rows=.N),by=type]
     attributes(r)$tzone<-India
 }
    r
}

mlogs<-function(mdh1=0,mdh2=0,file="clean_dump.txt",width=NULL,dt=NULL,xmpp=F,query_detail=1,query_only=T,logid=NULL,serial=NULL,verbose=F){
  w<-ifelse(is.null(width),getOption("width")-70,width)
  if (is.null(dt)) 
    {
    if(mdh1>0 & mdh2>0){
    start<-ymd_h(paste0("2018",mdh1),tz = India)
    end<-  ymd_h(paste0("2018",mdh2),tz = India)
    int_par<-interval(start,end)
    }else  stop("Mandtory parameters missing")
    }
    cat(paste("Starting to read", file,"...\n"))
    dump2<-suppressWarnings(fread(file,col.names = c("dt","id","type","command"),fill = T))
    if (nrow(dump2)<2) {
      message("Error : nothing to process from the file")
      return("Error : nothing to process from the file")
    }
    if(is.null(dt)){
    dump2$dt<-ymd_hms(dump2$dt,tz = India)
    range_in_dump<-range(dump2$dt)
    dump_interval<-interval(range_in_dump[1],range_in_dump[2])
    if(!int_overlaps(int_par,dump_interval)) {
      message("Error: The MySQL dump date range does not overlap with the date range in the parameter")
      str<-sprintf("The MySQL dump saved in '%s' has a date range of: %s",file,dump_interval)
      message(str)
      return("Error")
    }
    if(query_only) dump3<-dump2 %>% filter(int_overlaps(int_par,dt %--% dt),type=="Query") else 
      dump3<-dump2 %>% filter(int_overlaps(int_par,dt %--% dt))
    dump2$command<-str_trim(dump2$command)
    str_remove<-"ZZZZ"
    if(!xmpp) str_remove<-paste0(str_remove,"|\\{title|pubsub|sr_group|sr_user|insert into archive|UPDATE last|query_cache_type|muc|roster|motd|jid")
    if(query_detail<=3) str_remove<-paste0(str_remove,"|delete from live_session_status|utf|commit|begin")
    if(query_detail<=2 ) str_remove<-paste0(str_remove,"|update event_log")
    if(query_detail<=1) str_remove<-paste0(str_remove,"|select|SELECT")
    if (is.null(serial)) 
      value<- dump3 %>%  
      filter(!grepl(str_remove,command,ignore.case = T)) %>% 
      mutate(service_name=toupper(gsub("(\\w+)(.*)","\\1",command)),woq=gsub("`","",command),
             sqltable=tolower(
               case_when(
                 service_name=="UPDATE" ~ gsub("\\bupdate\\s(\\w+).*","\\1",command,ignore.case = T),
                 service_name=="SELECT" & (grepl("from",ignore.case = T,command)) ~ gsub(".*from\\s+\\W?(\\w+).*","\\1",command,ignore.case = T),
                 service_name=="INSERT" ~ gsub("insert into ([A-z.]+)[( ]+\\w+.*","\\1",woq,ignore.case = T),
                 service_name=="DELETE" ~ gsub("delete from ([A-z.]+)[( ]+\\w+.*","\\1",woq,ignore.case = T)
               )
             ),
             contents=str_trunc(woq,width = w)
      ) %>% select (-command,-woq) %>% as.data.table()
  } else 
    value<- dump2[id==logid,command]
  value
}

comblog<-function(mdh1=111215,mdh2 =101316,mysql=F,
                  xmpp_detail=2, mysql_detail=2,refresh=F,clean_file="clean_dump.txt", max_lines=100000){
 #----Initialize variables -----
    parrange<-range(ymd_h(paste0('2018',mdh1),tz = India),ymd_h(paste0('2018',mdh2),tz = India))
    int_par<-interval(parrange[1],parrange[2])
    xapi    <-  data.table()
    ymysql  <-  data.table()
    zxmpp   <-  data.table()
    if(refresh & mysql){
        file.info(clean_file)$mtime->last_modified
        print(paste("Time when last dump of clean_file was taken :",now()-last_modified))
        if(now()-last_modified > dseconds(30) ) {
            cat("starting to dump file from AWS server...") 
            download_log(file_name = MYSQL_LOG,jabber = F)->dump1
            cat("completed file dump\nstarted cleaning of log...")
            clean_log(dump = dump1)
            cat("completed cleaning\n")
        }
        
    }
 
    #--------- Extract API logs into datatable xapi -----
    xapi<- api_log(start_mdh = mdh1,end_mdh = mdh2,json_recency = ifelse(refresh,0,120)) %>% rename(type=API)
  # x1<- api_archive(start = mdh1,end=mdh2,truncated = F)
    # if(is.data.frame(x1) && nrow(x1)>0)
    #     x1[,type:="API-XML"][,.(time,service_name,type,user_id,input_string,contents)] else
    #         return(data.table())
    #attributes(x1$time)$tzone<-India
  
  # x2<-json_logs(last_update=ifelse(refresh,0,1200))
  #     x2<- x2[,type:="API-JSON"][,input_string:=gsub("\\s+","",parameters)
  #                                ][,contents:=NA_character_
  #                                  ][int_overlaps(int_par,time %--% time)
  #                                    ][,-c("parameters")]
      #select(time,service_name,user_id,input_string,type)
  # xapi<-bind_rows(x1,x2)
  message(sprintf("%d:number of rows read in from API log",nrow(xapi)))
  
   
  
    #--------- Extract MYSQL Logs into datatable ymysql ----
  ymysql<-data.table()
  if(mysql){
      ymysql<-mlogs(mdh1=mdh1,mdh2=mdh2,query_detail = mysql_detail,width = getOption("width")-130,query_only = ifelse(mysql_detail<3,T,F)) %>% as.data.table()
      if(nrow(ymysql)==1 && ymysql=="Error") return(message("MySQL Logs missing.. aborting"))
      if(!is.null(nrow(ymysql)) && nrow(ymysql)>0){
          ymysql %<>% select(time="dt",everything()) %>% mutate(type="MySQL") %>% as.data.table()
          mysqllogrange<-ymysql$time %>% range
          int_mysql<-interval(mysqllogrange[1],mysqllogrange[2])
          if(!int_overlaps(int_mysql,int_par)) {
              message("MySQL log range and date parameters donot overlap")
              return("Error")
          }
      }
      message(sprintf("%d:number of rows extracted from MySQL log",nrow(ymysql)))
  }
 
  
    #----------Extract XMPP Logs into datatable zxmpp -------
    xmpp_rows <- switch(xmpp_detail,
      xmpp_archive(mdh1 = mdh1,mdh2 = mdh2,refresh = T),
      xmpp_new(mdh1 = mdh1,mdh2 = mdh2)
                   ) 
  if(!is.null(nrow(xmpp_rows)) && nrow(xmpp_rows)>0) {
      zxmpp<-  xmpp_rows[,type:="XMPP"][,.(time=created_at,id,user_id=as.character(from),service_name=label,type,from,to,code,contents=body)]
      message(sprintf("%d:number of rows in XMPP extract",nrow(zxmpp)))
  } else {
      message("Zero rows returned by xmpp_archive function")
      zxmpp<-data.table()
  }

  
 
  #--- Combine all three logs - xapi,ymysql,zxmpp -----
  merged_log<- rbind(xapi,ymysql,zxmpp,fill=T)
  # if(mysql){
  #     if(nrow(xapi)>0 & nrow(ymysql)>0) {
  #         j1<-merge(xapi,ymysql,all=T)
  #         message(sprintf("%d:number of rows in API and MySQL combined dataframe",nrow(j1)))
  #     }
  # }
  # 
  # if(nrow(zxmpp)>0) {
  #     if(mysql)
  #         j2<-merge(j1,zxmpp,all=T) else
  #         j2<-merge(xapi,zxmpp,all=T) # bind the datatables together into one combined datatable
          
      message(sprintf("%d:number of total rows in the final log after all merges",nrow(merged_log)))
      
      # if(mysql) j2[,.(time,type,id,service_name,user_id,code,from,to,input_string,contents,sqltable)][order(time,-type,id)] else
      #     j2[,.(time,type,id,service_name,user_id,code,from,to,input_string,contents)][order(time,-type,id)]
          # arrange(time,desc(type),id) %>% 
          # select(time,type,id,service_name,user_id,code,from,to,input_string,contents,table) %>% 
          # as.data.table()
      
     #merged_log[,.(time,id,type,service_name,user_id,code,from,to,contents,status,input_len,input_string,response_delay,output_dim1,output_dim2,xml_input,xml_output)][order(time,-type,id)]
     merged_log[,-c("xml_input","xml_output"),with=F][order(time,-type,id)]
  }

  


trim_log<-function(x=NULL,sthm=NULL,endhm=NULL,mysql=F){
    if(any(is.null(st),is.null(end),is.null(df))) return("Error")
    stdt<-x$time[1]
    prefixdt<-paste0(year(stdt),month(stdt),day(stdt))
    st<-ymd_hm(paste0(prefixdt,sthm),tz = India)
    end<-ymd_hm(paste0(prefixdt,endhm),tz = India)
    dt1<-as.data.table(x)
    dt2<-copy(dt1[time %between% c(st,end)])
   setkey(dt2,time)
    start_row<-first(grep("XML|JSON",dt2$type))
    if(mysql) last_row<-last(grep("MySQL",dt2$type)) else
        last_row<-nrow(dt2)
    if(last_row==0) last_row<-nrow(dt2)
    dt2[,datetext:=as.character(time)][start_row:last_row]
}

proc_cl<-function(df=NULL,summlev=2,mysql=F){
    if(exists("df$sqltable")) DT<-copy(as.data.table(df)) else
        DT<-df %>% mutate(sqltable=NA) %>% as.data.table()
    setkey(DT,time,id) 
    DT[is.na(id),id:=as.integer(time)]
    DT[,break_cat:=rleid(type,prefix = "block")]    #run line encoding on type
    DT[,break_serv:=rleid(service_name,prefix = "batch" ),by=break_cat]  #rle on service and type
    DT[,shftime:=shift(time,1)] # add one column with a lag in time col
    DT[,gap:=(time-shftime)]
    DT[,bunch:=rleid(trunc(gap))]
    DT[,bunch2:=ifelse(trunc(gap)>0 & trunc(shift(gap,1))==0,bunch+1,bunch)]
    if(summlev==1) {
        message("returning level 1 summary")
        ifelse(mysql,begin<-c("time","type","id","gap","bunch2","service_name","sqltable"),begin<-c("time","type","id","gap","bunch2","service_name"))
        setcolorder(DT,c(begin,setdiff(names(DT),begin)))
        return(DT[1:nrow(DT)]) } else
         
        if(summlev==2){
            log_bunches<- DT[,.(services=.(unique(service_name)),tables=.(unique(.SD$sqltable[complete.cases(.SD$sqltable)])),ids=.(sort(unique(id))),time_start=first(time),type=first(type),count=.N),
                             by=.(bunch2)]
            log_bunches[,gap:=round(time_start-shift(time_start),1)][,datetext:=as.character(time_start)]
            return(log_bunches[,.(time_start,gap,type,count,services,tables,ids,datetext)])
        }
}

slow_query<-function(relative_path="/logs/slow.log",df=NULL){
    file = paste0(ip,relative_path)
    #if(is.null(df)) read.csv(file,header = F,sep = "\t",stringsAsFactors = F,fill = T,strip.white = T,blank.lines.skip = T) 
    if(is.null(df)) scan(file, what = character(), sep = "\n",fill = T,strip.white = T,blank.lines.skip = T,multi.line = T) 
}

ejabber_logs<-function(x=NULL){
    x %>% filter(grepl("sql",X1)) %>% select(1) ->sql_log
    x %>% filter(grepl("2018",X1)) -> datelog
    datelog %<>% mutate(time=ymd_hms(paste(datelog$X1,datelog$X2)))
    datelog %>% select(time,X4) %>% group_by(Minute=sprintf("%02d%3s:%02d:%02d",day(time),month(time,label = T),hour(time),minute(time)),field=X4) %>% ungroup()
}

ejd_term<-function(screen_string=scrcopy){
    fread(screen_string,header = F) ->x1
    x1[,.(sess=as.integer(str_extract(string = V8,pattern = "\\d++")))]->x2
    b$sessions100[x2,on=.(class_session_id=sess)]
}


read_capi<-function(no_lines=14690, pattern="public", local_file="/Applications/MAMP/htdocs/jupiter/commonAPI.php"){
    x<-readLines(con = local_file,n = no_lines) %>% gsub("\\s+"," ",.)
    x<-data.frame(a=x) %>% filter(grepl("public",a)) %>% .[,1]
    y1<-sapply(x, function(x){ str_match_all(x,"[A-Za-z_0-9]+")[[1]][,1] %>% unique()})
    y2<-c("null","public","function")
    y<-lapply(y1,setdiff,y2)
    parameters<-lapply(y,function(y){y[2:length(y)]})
    names(parameters)<-sapply(y,function(y){y[1]})
    parameters
}

search_api<-function(pattern=NULL){
    no_lines=14690
    local_file="/Applications/MAMP/htdocs/jupiter/commonAPI.php"
    x<-readLines(con = local_file,n = no_lines) %>% gsub("\\s+"," ",.)
    x<-data.frame(a=x) %>% filter(grepl("public",a)) %>% .[,1]
    y1<-sapply(x, function(x){ str_match_all(x,"[A-Za-z_0-9]+")[[1]][,1] %>% unique()})
    y2<-c("null","public","function")
    y<-sapply(y1,setdiff,y2)
    y
    }

read_code<-function(api="SaveMyLocation", local_file="/Applications/MAMP/htdocs/jupiter/commonAPI.php"){
    x<-readLines(con = local_file) %>% gsub("\\s+"," ",.)
    starts_at <- grep(api,x)
    x %>% readLines(con = local_file) %>% gsub("\\s+"," ",.)
    ends_at <- x %>% filter(row)
    while(grep("public",x))    
    y1<-sapply(x, function(x){ str_match_all(x,"[A-Za-z_0-9]+")[[1]][,1] %>% unique()})
    y2<-c("null","public","function")
    y<-lapply(y1,setdiff,y2)
    parameters<-lapply(y,function(y){y[2:length(y)]})
    names(parameters)<-sapply(y,function(y){y[1]})
    parameters
}


apif<-api_flow
apia<-api_archive
xa<-xmpp_archive
php<-php_log
cl<-comblog
pcl<-proc_cl    
