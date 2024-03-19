import mysql.connector
import csv
from datetime import datetime

cnx = mysql.connector.connect(user='cs5330', password='pw5330',
                              host='localhost',
                              database='dbprog')
cursor = cnx.cursor()



# fileName = "test1.csv"
file_path = input("Enter the file path: ")
with open(file_path, 'r') as file:
    csv_reader = csv.reader(file)
    for row in csv_reader:
        items = [item.strip() for item in row]  # Remove leading and trailing spaces
        print(items)
        try:
            if(items[0] == 'e'):
                query = ("""
                    CREATE TABLE IF NOT EXISTS Player (
                        ID INT PRIMARY KEY check (ID between 0 and 99999999),
                        Name VARCHAR(255) UNIQUE,
                        Birthdate DATE,
                        Rating INT,
                        State CHAR(2)
                    );
                """)
                cursor.execute(query)
                query = ("""
                CREATE TABLE IF NOT EXISTS Matches (
                    HostID INT,
                    GuestID INT,
                    Start DATETIME,
                    End DATETIME,
                    Hostwin BOOLEAN,
                    PreRatingHost INT,
                    PostRatingHost INT,
                    PreRatingGuest INT,
                    PostRatingGuest INT,
                    PRIMARY KEY (HostID, GuestID, Start),
                    FOREIGN KEY (HostID) REFERENCES Player(ID) ON DELETE CASCADE,
                    FOREIGN KEY (GuestID) REFERENCES Player(ID) ON DELETE CASCADE
                );
                """)
                cursor.execute(query)
            elif(items[0] == 'r'):
                clearTableQuery = "Delete FROM Player"
                cursor.execute(clearTableQuery)
                cnx.commit()
                clearTableQuery = "Delete FROM Matches"
                cursor.execute(clearTableQuery)
                cnx.commit()
            elif(items[0] == 'p'):
                insert_query = """INSERT INTO Player (ID, Name, Birthdate, Rating, State) 
                                VALUES (%s, %s, %s, %s, %s)"""
                
                data = (items[1], items[2], items[3], items[4], items[5])
                # print(data)
                cursor.execute(insert_query, data)
                cnx.commit()
            elif(items[0] == 'm'):
                insert_query = """INSERT INTO Matches (HostID, GuestID, Start, End, Hostwin, PreRatingHost, PostRatingHost, PreRatingGuest, PostRatingGuest) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                startDateTime = datetime.strptime(items[3], "%Y%m%d:%H:%M:%S")
                startStandardDateTime = startDateTime.strftime("%Y-%m-%d %H:%M:%S")
                endDateTime = datetime.strptime(items[4], "%Y%m%d:%H:%M:%S")
                endStandardDateTime = endDateTime.strftime("%Y-%m-%d %H:%M:%S")
                data = (items[1], items[2], startStandardDateTime, endStandardDateTime, items[5], items[6], items[7], items[8], items[9])

                cursor.execute(insert_query, data)
                cnx.commit()
            elif(items[0] == 'n'):
                insert_query = """INSERT INTO Matches (HostID, GuestID, Start, End, Hostwin, PreRatingHost, PostRatingHost, PreRatingGuest, PostRatingGuest) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                startDateTime = datetime.strptime(items[3], "%Y%m%d:%H:%M:%S")
                startStandardDateTime = startDateTime.strftime("%Y-%m-%d %H:%M:%S")
                data = (items[1], items[2], startStandardDateTime, None, None, None, None, None, None)

                cursor.execute(insert_query, data)
                cnx.commit()
            elif(items[0] == 'c'):
                update_query = """UPDATE Matches SET End = %s, HostWin = %s, PreRatingHost = %s, PostRatingHost = %s, PreRatingGuest = %s, PostRatingGuest = %s WHERE HostID = %s AND GuestID = %s AND Start = %s"""
                startDateTime = datetime.strptime(items[3], "%Y%m%d:%H:%M:%S")
                startStandardDateTime = startDateTime.strftime("%Y-%m-%d %H:%M:%S")
                endDateTime = datetime.strptime(items[4], "%Y%m%d:%H:%M:%S")
                endStandardDateTime = endDateTime.strftime("%Y-%m-%d %H:%M:%S")
                data = (endStandardDateTime,items[5],items[6],items[7],items[8], items[9],items[1],items[2],startStandardDateTime)
                cursor.execute(update_query, data)
                cnx.commit()
            elif(items[0] == 'P'):
                select_query = """SELECT * FROM Player where ID = %s"""
                data = (items[1],)
                cursor.execute(select_query,data)
                for item in cursor:
                    formatted_birthdate = item[2].strftime("%Y-%m-%d")  # Format the birthdate
                    print(f"{item[0]}, {item[1]}, {formatted_birthdate}, {item[3]}, {item[4]}")
            elif(items[0] == 'A'):
                #given a player, listed the win-loss against other
                #players that he/she has played against.
                select_query = """  SELECT * FROM Player
                                    WHERE ID = %s"""
                data = (items[1],)
                cursor.execute(select_query,data)
                for item in cursor:
                    print(f"{item[0]}, {item[1]}")



                select_query = """  SELECT GuestID, HostWin, name
                                    FROM Matches 
                                    JOIN Player ON GuestID = ID
                                    WHERE HostID = %s
                                    UNION ALL
                                    SELECT HostId, NOT HostWin, name
                                    FROM Matches 
                                    JOIN Player ON HostID = ID
                                    WHERE GuestID = %s
                                """
                data = (items[1],items[1])
                cursor.execute(select_query,data)
                winLoss = {}
                for item in cursor:
                    opponent_id = item[0]
                    win = item[1]
                    if opponent_id not in winLoss:
                        winLoss[opponent_id] = {"wins": 0, "losses": 0}
                    if win:
                        winLoss[opponent_id]["wins"] += 1
                    else:
                        winLoss[opponent_id]["losses"] += 1
                    winLoss[opponent_id]["name"] = item[2]
                for item in winLoss:
                    print(f"{item}, {winLoss[item]['name']}, Wins Against: {winLoss[item]['wins']}, Losses Against: {winLoss[item]['losses']}")
            elif(items[0] == 'D'):
                # given a start date and end date, list all matches 
                # in chronological order of start time , break
                # ties with the hostID.
                select_query = """  SELECT start, end, hostWin, Host.Name, Guest.Name
                                    FROM Matches
                                    JOIN Player AS Host ON Matches.hostId = Host.ID
                                    JOIN Player AS Guest ON Matches.GuestID = Guest.ID
                                    WHERE start > %s
                                    AND start < %s
                                    ORDER BY start,hostID
                                    """
                data = (items[1],items[2])
                cursor.execute(select_query,data)
                for item in cursor:
                    winner = 'H' if item[2] else 'G'


                    print(f"{item[0]}, {item[1]}, {item[3]}, {item[4]}, {winner}")
            elif(items[0] == 'M'):
                # return the matches by a certain player (regardless of 
                # whether it has been played), listed in
                # chronological order
                select_query = """  SELECT * FROM Player
                                    WHERE ID = %s"""
                data = (items[1],)
                cursor.execute(select_query,data)
                for item in cursor:
                    print(f"{item[0]}, {item[1]}")
                
                select_query = """  SELECT start, end, GuestID, Name, hostWin, PostRatingHost, preRatingHost
                                    FROM Matches
                                    JOIN Player ON Matches.GuestId = Player.ID
                                    WHERE hostId = %s
                                    UNION ALL
                                    SELECT start, end, HostID, Name, NOT hostWin, PostRatingGuest, preRatingGuest
                                    FROM Matches
                                    JOIN Player ON Matches.HostID = Player.ID
                                    WHERE GUESTID = %s
                                    ORDER BY start
                                    """
                data = (items[1],items[1])
                cursor.execute(select_query,data)
                prevRating = 0
                wasTherePrevRating = False
                for item in cursor:
                    if(wasTherePrevRating):
                        #check if prev matches current preRating and set prevRating
                        if(prevRating != item[6]):
                            print("inconsistent rating: ", end="")
                    if(item[5] != None):
                        prevRating = item[5]
                        wasTherePrevRating = True
                    else:
                        wasTherePrevRating = False
                    win_loss = 'W' if item[4] else 'L'

                    print(f"Start: {item[0]}, End: {item[1]}, Opponent ID: {item[2]}, Opponent Name: {item[3]}, Win/Loss: {win_loss}, Post-match Rating: {item[5]}")
            print()
        
        except (mysql.connector.Error, ValueError) as err:
            print(f"{row}, Input Invalid: {err}")






# insert_query = """INSERT INTO Player (ID, Name, Birthdate, Rating, State) 
#                   VALUES (%s, %s, %s, %s, %s)"""
# data = (2, 'howdy', '1990-01-01', 2000, 'NY')

# cursor.execute(insert_query, data)
# cnx.commit()

# query = ("SELECT * FROM Player")
# cursor.execute(query)
# for row in cursor:
#     print(row)
# query = ("SELECT * FROM Matches")
# cursor.execute(query)
# for row in cursor:
#     print(row)