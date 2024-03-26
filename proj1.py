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
        items = [i.replace(":","").strip() for i in row]
        # items = [item.strip() for item in row]  # Remove leading and trailing spaces
        # print(items)
        #need to put custom output instead of the line, also if sql find is bad 
        try:
            if(items[0] == 'e'):
                #check that not null
                # query = "DROP TABLE Player;"
                # cursor.execute(query)
                # query = "DROP TABLE Matches;"
                # cursor.execute(query)
                
                print("Creating Tables if they don't exist")
                query = ("""
                    CREATE TABLE IF NOT EXISTS Player (
                        ID INT UNSIGNED PRIMARY KEY CHECK (ID between 0 and 99999999),
                        Name VARCHAR(255) NOT NULL UNIQUE,
                        Birthdate DATE NOT NULL,
                        Rating INT NOT NULL CHECK (Rating >= 100),
                        State CHAR(2) NOT NULL 
                    );
                """)
                cursor.execute(query)
                #make ratings unsigned
                #check that all 4 ratings are all >= 100
                query = ("""
                CREATE TABLE IF NOT EXISTS Matches (
                    HostID INT UNSIGNED NOT NULL,
                    GuestID INT UNSIGNED NOT NULL,
                    Start DATETIME NOT NULL,
                    End DATETIME,
                    HostWin BOOLEAN,
                    PreRatingHost INT UNSIGNED CHECK (PreRatingHost >= 100),
                    PostRatingHost INT UNSIGNED CHECK (PostRatingHost >= 100),
                    PreRatingGuest INT UNSIGNED CHECK (PreRatingGuest >= 100),
                    PostRatingGuest INT UNSIGNED CHECK (PostRatingGuest >= 100),
                    PRIMARY KEY (HostID, GuestID, Start),
                    FOREIGN KEY (HostID) REFERENCES Player(ID) ON DELETE CASCADE,
                    FOREIGN KEY (GuestID) REFERENCES Player(ID) ON DELETE CASCADE
                );
                """)
                # cursor.execute(query)
                cursor.execute(query)
            elif(items[0] == 'r'):
                print("Clearing Tables")
                clearTableQuery = "Delete FROM Player"
                cursor.execute(clearTableQuery)
                clearTableQuery = "Delete FROM Matches"
                cursor.execute(clearTableQuery)
                cnx.commit()
            elif(items[0] == 'p'):
                print("Inserting Player")
                insert_query = """INSERT INTO Player (ID, Name, Birthdate, Rating, State) 
                                VALUES (%s, %s, %s, %s, %s)"""
                
                data = (items[1], items[2], items[3], items[4], items[5])
                # print(data)
                cursor.execute(insert_query, data)
                cnx.commit()
            elif(items[0] == 'm'):
                print("Inserting Full Match")
                insert_query = """INSERT INTO Matches (HostID, GuestID, Start, End, HostWin, PreRatingHost, PostRatingHost, PreRatingGuest, PostRatingGuest) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                data = (items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], items[9])

                cursor.execute(insert_query, data)
                cnx.commit()
            elif(items[0] == 'n'):
                print("Inserting Incomplete Match")
                insert_query = """INSERT INTO Matches (HostID, GuestID, Start) 
                                VALUES (%s, %s, %s)"""
                data = (items[1], items[2], items[3])
                cursor.execute(insert_query, data)
                cnx.commit()
            elif(items[0] == 'c'):
                print("Updating Match")
                update_query = """UPDATE Matches SET End = %s, HostWin = %s, PreRatingHost = %s, PostRatingHost = %s, PreRatingGuest = %s, PostRatingGuest = %s WHERE HostID = %s AND GuestID = %s AND Start = %s"""
                data = (items[4],items[5],items[6],items[7],items[8], items[9],items[1],items[2],items[3])
                cursor.execute(update_query, data)
                cnx.commit()
            elif(items[0] == 'P'):
                print("Selecting Player")
                select_query = """SELECT * FROM Player where ID = %s"""
                data = (items[1],)
                cursor.execute(select_query,data)
                for item in cursor:
                    formatted_birthdate = item[2].strftime("%Y-%m-%d")  # Format the birthdate
                    print(f"{item[0]}, {item[1]}, {formatted_birthdate}, {item[3]}, {item[4]}")
            elif(items[0] == 'A'):
                print("Win-Loss of Player")
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
                print("History of Matches")
                # given a start date and end date, list all matches 
                # in chronological order of start time , break
                # ties with the hostID.
                select_query = """  SELECT start, end, HostWin, Host.Name, Guest.Name
                                    FROM Matches
                                    JOIN Player AS Host ON Matches.hostId = Host.ID
                                    JOIN Player AS Guest ON Matches.GuestID = Guest.ID
                                    WHERE start >= %s
                                    AND start < DATE_ADD(%s, INTERVAL 1 DAY)
                                    ORDER BY start,hostID
                                    """
                data = (items[1],items[2])
                # data = (items[1], datetime.strptime(items[2], '%Y%m%d') + timedelta(days=1))

                cursor.execute(select_query,data)
                for item in cursor:
                    winner = 'H' if item[2] else 'G'


                    print(f"{item[0]}, {item[1]}, {item[3]}, {item[4]}, {winner}")
            elif(items[0] == 'M'):
                print("History of: ")
                # return the matches by a certain player (regardless of 
                # whether it has been played), listed in
                # chronological order
                select_query = """  SELECT * FROM Player
                                    WHERE ID = %s"""
                data = (items[1],)
                cursor.execute(select_query,data)
                for item in cursor:
                    print(f"{item[0]}, {item[1]}")
                
                select_query = """  SELECT start, end, GuestID, Name, HostWin, PostRatingHost, preRatingHost
                                    FROM Matches
                                    JOIN Player ON Matches.GuestId = Player.ID
                                    WHERE hostId = %s
                                    UNION ALL
                                    SELECT start, end, HostID, Name, NOT HostWin, PostRatingGuest, preRatingGuest
                                    FROM Matches
                                    JOIN Player ON Matches.HostID = Player.ID
                                    WHERE GuestID = %s
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
                            print("Inconsistent rating: ", end="")
                    if(item[5] != None):
                        prevRating = item[5]
                        wasTherePrevRating = True
                    else:
                        wasTherePrevRating = False
                    win_loss = 'W' if item[4] else 'L'

                    print(f"Start: {item[0]}, End: {item[1]}, Opponent ID: {item[2]}, Opponent Name: {item[3]}, Win/Loss: {win_loss}, Post-match Rating: {item[5]}")
            print()
        
        except mysql.connector.Error as err:
            if(err.sqlstate == '23000'):
                print(f"{row}, Entry already exists. Error message: {err}")
            else:
                print(f"{row}, Input Invalid: {err}")
        except ValueError as err:
            print(f"{row}, Input Invalid: {err}")