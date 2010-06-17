#!/usr/bin/python
""" given a list of 'events', compute the tanimoto similarity across all items
    input data must be in the form (item,user)
    formula used is tanimoto, which is a variant of jaccard.  
    τ = NAB / NA + NB - NAB """
    
import sys
import MySQLdb

# the list rows[] stores the original source data, whether its from a database or .csv file.
rows=[]


users={}
items=[]

# minimumOverlapThreshhold is a filter that will restrict items based on
# a minimum number of times they've been rated together.
# This will be set based on the type of item rated.
minimimOverlapThreshhold=5

# useSQL directs the program to use a sql database.  otherwise we'll
# use a text file as input.
# either way, the input must be in the format (item, user)
useSQL=True

intersections={}
itemusers={}
userlist=[]
relatedItems={}

if useSQL:
  conn = MySQLdb.connect (host = "localhost",user="root",db="variety")
  cursor = conn.cursor()
  
  # data must be in the form (item_id, user_id)
  # this is my first attempt at mysql and python.  i'm building a result set
  # then looping through the cursor and appending (item_id,user_id) to a list called rows
  # this method is from the documentation, I dont know if any more efficient ones exist.
  sql="select histogram_value ,account_id from variety.histogram_archive order by histogram_value limit 10000"
  cursor.execute(sql)
  while (1):
    row = cursor.fetchone()
    if row == None:
      break
    rows.append((int(row[0]),row[1]))

else:  
  for line in file('histogram_sorted.csv'):
  #for line in file(sys.argv[1]):
     a,b=line.strip().split(',')
     a=int(a)
     rows.append((a,b))

rows.sort()

last_item = None
last_user = None


# build the list of items
# also build a dict of users and items rated

for item,user in rows:

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
    if cnt >= minimimOverlapThreshhold:
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

