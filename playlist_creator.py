#  Copyright 2024 Kasun Bamunuarachchi <knimantha817@gmail.com> <kasun.bamunuarachchi@outlook.com>
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http ://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

####################################
#
#   Automatic playlist generator
#   Kasun Nimantha Bamunuarachchi
#   Execution Command: exec(open("filepath/208556B_playlist_creator.py").read())
#   Dataset source: https://www.kaggle.com/yamaerenay/spotify-dataset-19212020-160k-tracks?select=tracks.csv
#
####################################
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType,DateType,ArrayType
from pyspark.sql.functions import col,round,lit

songcount = 0

columns_to_drop = ['popularity','duration_ms','explicit',   \
                    'id_artists','danceability','energy',   \
                    'loudness','mode','speechiness',        \
                    'acousticness','instrumentalness',      \
                    'liveness','valence','time_signature']

audioschema = StructType().add("id",StringType(),True) \
                    .add("name",StringType(),True) \
                    .add("popularity",IntegerType(),True) \
                    .add("duration_ms",IntegerType(),True) \
                    .add("explicit",IntegerType(),True) \
                    .add("artists",StringType(),True) \
                    .add("id_artists",StringType(),True) \
                    .add("release_date",DateType(),True) \
                    .add("danceability",DoubleType(),True) \
                    .add("energy",DoubleType(),True) \
                    .add("key",IntegerType(),True) \
                    .add("loudness",DoubleType(),True) \
                    .add("mode",IntegerType(),True) \
                    .add("speechiness",DoubleType(),True) \
                    .add("acousticness",DoubleType(),True) \
                    .add("instrumentalness",DoubleType(),True) \
                    .add("liveness",DoubleType(),True) \
                    .add("valence",DoubleType(),True) \
                    .add("tempo",DoubleType(),True) \
                    .add("time_signature",IntegerType(),True)

def printDataFrame(dataFrame,schema,rows):
    dataFrame.show(n=rows)
    if schema == True:
            dataFrame.printSchema()
    else:
        return

def createSession():
    return SparkSession.builder.appName("Playlist-gen").config("spark.some.config.option","some-value").getOrCreate()

def parseCSVtoDF(sc,file_path):
    return sc.read.option("header",True).schema(audioschema).csv(file_path)

def queryValue(dataFrame,queryVal,column):
    return dataFrame.filter(col(column)==queryVal)

def dropColumns(dataFrame,columns):
   return dataFrame.drop(columns)

def roundOff(dataFrame,column,points):
    return dataFrame.withColumn(column,round(column,points))

def bpmFiltering(dataFrame,bpm):
    return queryValue(dataFrame,bpm,"tempo")

def keyFiltering(dataFrame,key):
    return queryValue(dataFrame,key,"key")

def stripByYear(dataframe,year):
    return dataframe.filter(col("release_date")>=lit(year))

def selectSong(dataFrame,name):
    print('Entered title: ',name)
    return queryValue(dataFrame,name,"name")

############# Initiate  dataframe ##################
print('----------------------------------------------------------------\n')
print('                       Playlist creator\n')

sprk = createSession()
audioDataframe = roundOff(parseCSVtoDF(sprk,"tracks/tracks.csv"),"tempo",0)
print('Total number of rows in dataset:',audioDataframe.count())
print('\n')

##############  Song selection  ####################

while songcount != 1:
    if songcount == 0:
        title = input("Enter a valid song title: ")
        songResult = selectSong(audioDataframe,title)
        printDataFrame(songResult,False,20)
        songcount = songResult.count()
    elif songcount > 1:
        selected = int(input("Select the row number: "))
        sid = songResult.collect()[selected-1][0]
        songResult = songResult.filter(col("id")==sid)
        printDataFrame(songResult,False,10)
        songcount = songResult.count()
        
############  Collect song details  #################
bpm = songResult.collect()[0][18]
key = songResult.collect()[0][10]
spid = songResult.collect()[0][0]
print('Selected song is: ',songResult.collect()[0][1],' by: ',songResult.collect()[0][5])
print('Spotify ID of selected song: ',spid)
print('BPM of selected song: ',bpm)
print('Key of selected song: ',key)
print('\n')

#############  Playlist Generation  #################
songResult.unpersist()
recomPlaylist = stripByYear(keyFiltering(bpmFiltering(audioDataframe,bpm),key),"2005-01-01")
print('------------ Recommended Playlist ------------\n')
printDataFrame(recomPlaylist,False,25)
#audioDataframe.unpersist()
#queryOut.unpersist()
