#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[1]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[2]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[3]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[4]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[5]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[6]:


# Create a keyspace with replication 
try:
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS event_data
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")
except Exception as e:
    print(e)


# #### Set Keyspace

# In[7]:


try:
    session.set_keyspace('event_data')
except Exception as e:
    print(e)


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[8]:


## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

# In order to perform this query we need information about the sessions and the songs listened to, 
# so the table that will be build will be composed of the following columns: `sessionId`, `itemInSession`, `artist`, 
# `song`, and `length`. The `PRIMARY_KEY` is composed by `sessionId` as partition key and `itemInSession` as the 
# clustering columns. This allows to have a unique key that responds to the query requirements.

query = "CREATE TABLE IF NOT EXISTS music_library_song_info "
query = query + "(sessionId int, itemInSession int, artist text, song text, length float, "                 "PRIMARY KEY (sessionId, itemInSession))"

try:
    session.execute(query)
except Exception as e:
    print(e)        


# In[9]:


file = 'event_datafile_new.csv'

# Insert data into the table from CSV.

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO music_library_song_info (sessionId, itemInSession, artist, song, length)"
        query = query + " VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[10]:


# Verify the data was entered into the table.

query = "SELECT artist, song, length FROM music_library_song_info WHERE sessionId=338 AND itemInSession=4"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
df = pd.DataFrame(rows._current_rows)
df


# In[11]:


# Use PrettyTable to display data in tabular form and include headings

from prettytable import PrettyTable

t = PrettyTable(['Artist', 'Song', 'Length'])
for row in rows:
    t.add_row([row.artist, row.song, row.length])

print(t)


# In[12]:


## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

# In order to perform this query we need information about the name of artist and song sorted by item in session
# so the table that will be build will be composed of the following columns: `userId`, `sessionId`, `itemInSession`, 
# `artistName`, `songTitle`, `firstName` and `lastName`. The `PRIMARY_KEY` is composed of `userId` and `sessionId`
# as composite partition key and `itemInSession` as the clustering columns. 
# This allows to have a unique key that responds to the query requirements.

query = "CREATE TABLE IF NOT EXISTS music_library_user_info "
query = query + "(userId int, sessionId int, itemInSession int, artistName text, songTitle text, "                 "firstName text, lastName text, PRIMARY KEY((userId, sessionId), itemInSession))"
    
try:
    session.execute(query)
except Exception as e:
    print(e)


# In[13]:


# Insert data into the table from CSV.

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO music_library_user_info (userId, sessionId, itemInSession, artistName, "         "songTitle, firstName, lastName)"
        query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))


# In[14]:


# Verify the data was entered into the table.

query = "SELECT artistName, songTitle, firstName, lastName FROM music_library_user_info "         "WHERE userId=10 AND sessionId=182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

df = pd.DataFrame(rows._current_rows)
df


# In[15]:


# Use PrettyTable to display data in tabular form and include headings

t = PrettyTable(['Artist Name', 'Song Title', 'First name', 'Last Name'])
for row in rows:
    t.add_row([row.artistname, row.songtitle, row.firstname, row.lastname])

print(t)


# In[16]:


## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

# In order to perform this query we need information about the users and the songs listened to, 
# so the table that will be build will be composed of the following columns: `songTitle`, `userId`, `firstName`, 
# and `lastName`. The `PRIMARY_KEY` is composed by `songTitle` and `userId` as partition key.
# This allows to have a unique key that responds to the query requirements.

query = "CREATE TABLE IF NOT EXISTS music_library_song_users_info "
query = query + "(songTitle text, userId int, firstName text, lastName text, PRIMARY KEY(songTitle, userId))"
    
try:
    session.execute(query)
except Exception as e:
    print(e)


# In[17]:


# Insert data into the table from CSV.

file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO music_library_song_users_info (songTitle, userId, firstName, lastName)"
        query = query + " VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))


# In[18]:


# Verify the data was entered into the table.

query = "SELECT firstName, lastName FROM music_library_song_users_info WHERE songTitle='All Hands Against His Own'"

try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
df = pd.DataFrame(rows._current_rows)
df


# In[19]:


# Use PrettyTable to display data in tabular form and include headings

t = PrettyTable(['First name', 'Last Name'])
for row in rows:
    t.add_row([row.firstname, row.lastname])

print(t)


# ### Drop the tables before closing out the sessions

# In[20]:


try:
    session.execute("DROP TABLE IF EXISTS music_library_song_info")
    session.execute("DROP TABLE IF EXISTS music_library_user_info")
    session.execute("DROP TABLE IF EXISTS music_library_song_users_info")
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[21]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




