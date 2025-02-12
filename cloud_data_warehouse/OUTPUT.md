```
Connecting to the RedShift Cluster Database
Count rows in each of the tables.

Query 1/7


Executing
    SELECT COUNT(*) FROM staging_events;

Number of rows:  [8056]

Query 2/7

Executing
    SELECT COUNT(*) FROM staging_songs;

Number of rows:  [14896]

Query 3/7

Executing
    SELECT COUNT(*) FROM songplays;

Number of rows:  [333]

Query 4/7

Executing
    SELECT COUNT(*) FROM users;

Number of rows:  [104]

Query 5/7

Executing
    SELECT COUNT(*) FROM songs;

Number of rows:  [14896]

Query 6/7

Executing
    SELECT COUNT(*) FROM artists;

Number of rows:  [10025]

Query 7/7

Executing
    SELECT COUNT(*) FROM time;

Number of rows:  [6813]

Five most playable.

Query 1/2

What are the 5 most played artists?
['Dwight Yoakam', 37]
['Kid Cudi / Kanye West / Common', 10]
['Ron Carter', 9]
['Lonnie Gordon', 9]
['B.o.B', 8]

Query 2/2

What are the five most played songs?
['Dwight Yoakam', "You're The One", 37]
['Ron Carter', "I CAN'T GET STARTED", 9]
['Lonnie Gordon', 'Catch You Baby (Steve Pitron & Max Sanna Radio Edit)', 9]
['B.o.B', "Nothin' On You [feat. Bruno Mars] (Album Version)", 8]
['Usher featuring Jermaine Dupri', "Hey Daddy (Daddy's Home)", 6]

Closing database connection.
```
