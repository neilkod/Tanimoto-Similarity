#!/usr/bin/python
""" given a list of 'events', compute the tanimoto similarity across all items
    input data must be in the form (item,user)
    formula used is tanimoto, which is a variant of jaccard.  
    T = NAB / NA + NB - NAB 
    an event is considered a user rating an item.
    """
    
import sys
import MySQLdb

# the list rows[] stores the original source data, whether its from a database or .csv file.
rows=[]

# dict users has user_id as the key and a list of rated items as the value
# example {'887C6CC8-22ED-42A1-A634-904253488F7C': [46, 49, 60], '6B98CBCD-FA7C-4C37-8105-43CCC83708EA': [34]}
users={}

# list items is simply a list of all item_id's that have been rated.
# example [1, 26, 30, 34, 35, 37, 39, 41]
items=[]

# minimumOverlapThreshhold is a filter that will restrict items based on
# a minimum number of times they've been rated together.
# This will be set based on the type of item rated.
minimimOverlapThreshhold=5

# useSQL directs the program to use a sql database.  otherwise we'll
# use a text file as input.
# either way, the input must be in the format (item, user)
useSQL=True

# dict intersections has item_id as the key and a list of user_ids who rated the item as the value
# example : { 26: ['19176B2C-033B-496E-A708-3FCD7C5A4558', '6E06D52F-0818-4ED7-8F28-2D90856B9878'] }
intersections={}

# userlist is a temporary list that stores the users who have rated a given item.
# as we loop through the items, we keep track of which userids have rated the item.
# we clear this list and start over when we move on to the next itemid.
userlist=[]

# dict relatedItems stores an item_id as the key and as the value stores a dictionary 
# containing a related item_id as the key and the count of times its been rated with the original item
# as the value
# example  { 83: {48: 2, 65: 1, 50: 1, 49: 1} }
# means that item 83 has been rated with item 48 twice, item 83 has been rated with 65 once,
# item 83 has been rated with items 50 and 49 once apiece.
relatedItems={}

def tanimotoSimilarity(ab,a,b):
 
  return ab / float(a + b -ab)
  
def readDbConf():
  """
  procedure readDbConf() reads a configuration file as input and
  returns a dictionary of items based on the configuration file.
  the file must be in the format keyword<tab>value
  and expects the following fields

  database  db_elpais_uy
  RO_host  127.0.0.1
  RO_password  123
  RO_username  root
  RW_host  127.0.0.1
  RW_password  123
  RW_username  root 
  """  
  parms = {}
  for line in file('local.conf'):
    k,v = line.strip().split()
    parms[k]=v
  if 'RO_password' not in parms.keys():
    parms['RO_password'] = ''
  if 'RW_password' not in parms.keys():
    parms['RW_password'] = ''
  return parms
  
if useSQL:
  parms = readDbConf()
  # we are using two separate databases - a read-only database for retrieving rated items
  # and a read/write database for storing the results.
  conn = MySQLdb.connect (host = parms['RO_host'],user=parms['RO_username'],db=parms['database'],passwd=parms['RO_password'])
  cursor = conn.cursor()
  
  # data must be in the form (item_id, user_id)
  # this is my first attempt at mysql and python.  i'm building a result set
  # then looping through the cursor and appending (item_id,user_id) to a list called rows
  # this method is from the documentation, I dont know if any more efficient ones exist.
  sql="select histogram_value ,account_id from variety.histogram_archive order by histogram_value limit 50000"
  cursor.execute(sql)
  while (1):
    row = cursor.fetchone()
    if row == None:
      break
    rows.append((int(row[0]),row[1]))
  
  # we're done, close the database connection  
  conn.close()
  
else:  
  for line in file('histogram_sorted.csv'):
  #for line in file(sys.argv[1]):
     a,b=line.strip().split(',')
     a=int(a)
     rows.append((a,b))

# this next step might not be necessary because the SQL takes care of sorting the rows
# to order them by item_id.  Additionally, when I use a .csv file as input, I try
# and pre-sort it by the item_id to save python from having to sort.

rows.sort()

# initialize last_item and last_user variables.  These are used
# to keep track of where we are inside of loops and to see if
# items or users have changed.
last_item = None
last_user = None


# build the list of items
# also build a dict of users and items rated

for item,user in rows:

  # add the user to the users dict if they don't already exist.
  users[user]=users.get(user,[])
  
  # append the current item_id to the list of items rated by the current user
  users[user].append(item)
  
  if item != last_item:
    # we just started a new item which means we just finished processing an item
    # write the userlist for the last item to the intersections dictionary.
    if last_item != None:
      intersections[last_item]=userlist

    userlist=[user]
  
    last_item = item
    items.append(item)
  else:
    userlist.append(user)

intersections[last_item]=userlist    



# now, for items, lets go through the list and find
#for item in items:
  #find the users who rated each item
  
print 'len intersections:%d ' % (len(intersections))
print 'len items: %d' % (len(items))



# we're going to store the results into a new database connection
# this time, we'll use the read/write connection
conn = MySQLdb.connect (host = parms['RW_host'],user=parms['RW_username'],db=parms['database'],passwd=parms['RW_password'])
cursor = conn.cursor()

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
        #tanimotoSim = cnt / float(len(intersections[key]) + len(intersections[b])-cnt)
        similarity = tanimotoSimilarity(cnt,len(intersections[key]),len(intersections[b]))

#        print "%d\t%d\t%d\t%d\t%d\t%f\t%f" % (key,b,cnt,len(intersections[key]),len(intersections[b]),(cnt / float((len(intersections[key]) + len(intersections[b])-cnt))),tanimotoSim)
#        print "%d\t%d\t%d\t%d\t%d\t%f" % (key,b,cnt,len(intersections[key]),len(intersections[b]),tanimotoSim)
      # this needs to be a sub-function

        sql= "insert into story_similarity(method,story_id,related_story_id,relevance,created) values(6,%d,%d,%f,curdate())"%(key,b,similarity)
        cursor.execute(sql)
#    print len(intersections[key])
#    print len(intersections[b])
conn.close()        

print "................................"


