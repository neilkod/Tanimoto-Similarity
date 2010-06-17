#!/usr/bin/python
import sys
import MySQLdb
rows=[]
users={}
items=[]
useSQL=True

intersections={}
itemusers={}
userlist=[]
relatedItems={}

if useSQL:
	conn = MySQLdb.connect (host = "localhost",user="root",db="variety")
	cursor = conn.cursor()
	sql="select histogram_value,account_id from variety.histogram_archive"
	cursor.execute(sql)
	while (1):
	  row = cursor.fetchone()
	  if row == None:
	    break
	  rows.append((int(row[0]),row[1]))
#	conn.close()
else:	
	for line in file('histogram_sorted.csv'):
	#for line in file(sys.argv[1]):
 		a,b=line.strip().split(',')
 		a=int(a)
 		rows.append((a,b))

rows.sort()





last_item = None
last_user = None

#####
# build the list of items
# also build a dict of users and items rated
for item,user in rows:#[0:500]:
#  print (item,user)
  users[user]=users.get(user,[])
  users[user].append(item)
  if item != last_item:
    # we just started a new item
    if last_item != None:
      intersections[last_item]=userlist

    userlist=[user]
  
    last_item = item
    items.append(item)
  else:
    userlist.append(user)
#  print 'item: %d user: %s userlist is %s' % (item,user,userlist)    
intersections[last_item]=userlist    

# now, for each item
# find 

    
    
#####

# now, for items, lets go through the list and find
#for item in items:
  #find the users who bought each item
  
print 'len intersections:%d ' % (len(intersections))
print 'len items: %d' % (len(items))
print len(users)
#print users
#print "intersections: %s" % intersections
#print intersections
#print intersections.keys()
#print "length of intersections 0 is: %d "%len(intersections[0])
#print "length of intersections 8 is: %d "%len(intersections[8])
#print "length of intersections 9 is: %d "%len(intersections[9])


"""
# build the list of items
for item,user in rows:
  if item != last_item:
    last_item = item
    users = [user]
    items.append(item)
  else:
    users.append(user)
    
"""    




#for row in rows:
#  print row
#intersections={}
#print intersections

for key,listOfReaders in intersections.iteritems():   #.iteritems():
  relatedItems[key]={}
  related=[]

  for ux in listOfReaders:
    for itemRead in users[ux]:
      if itemRead != key:
        if itemRead not in related:
          related.append(itemRead)
        relatedItems[key][itemRead]= relatedItems[key].get(itemRead,0) + 1
  for b,cnt in relatedItems[key].iteritems():
    if cnt >= 5:
        tanimotoSim = cnt / float(len(intersections[key]) + len(intersections[b])-cnt)
#        print "%d\t%d\t%d\t%d\t%d\t%f\t%f" % (key,b,cnt,len(intersections[key]),len(intersections[b]),(cnt / float((len(intersections[key]) + len(intersections[b])-cnt))),tanimotoSim)
#        print "%d\t%d\t%d\t%d\t%d\t%f" % (key,b,cnt,len(intersections[key]),len(intersections[b]),tanimotoSim)
      # this needs to be a sub-function

        sql= "insert into story_similarity(method,story_id,related_story_id,relevance,created) values(6,%d,%d,%f,curdate())"%(key,b,tanimotoSim)
        cursor.execute(sql)
#    print len(intersections[key])
#    print len(intersections[b])
          
conn.close()

#  print "items read with %d are %s" % (key,related)


  
#print relatedItems  
    
#  print item


#    print uid
#    print users[uid]
#    if item in users[uid]:
#      print "MAATCH!!"
    
    
  


  
  
#print intersections
print "................................"
#print users


"""  
last article=None
for article,user in rows:
  if article != last_article:
"""

