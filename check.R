dt1<-room_efficiency() #edits by SM for testing
dt1[,best_room := .SD[order(-utiliz)]$room[1], by=class
    ][,best_seat:=.SD[room==best_room,maxseat][1],by=class
      ][,.(room=first(best_room),stud=first(max_stud),seats=first(best_seat),utli=max(percent(max_stud/best_seat,0)),teacher=.(unique(teacher))),by=class]
#[seat_count:= .SD[room==best_room]$maxseat]

    
#     worst_room=.SD[order(utiliz)]$room[1]
#     stud_count=max(max_stud)
#     max_utiliz=max(utiliz)
#     min_utiliz=min(utiliz)
#     count_rooms=.N
# .(best_room,seat_count,worst_room,count_rooms,max_utiliz,min_utiliz) },by=class]