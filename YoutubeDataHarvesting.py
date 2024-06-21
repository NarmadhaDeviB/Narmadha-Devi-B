# Input Necessary Libraries 
from googleapiclient.discovery import build
import streamlit as st
import pandas as pd
import mysql.connector as sql
from streamlit_option_menu import option_menu

st.set_page_config(page_title="YouTube Data Harvesting and Warehousing",
                   layout="wide",
                   initial_sidebar_state ="auto",
                   menu_items={'About': "This was done by Narmadha Devi B"})


with st.sidebar:
    selected = option_menu(None, ["Home"," Extract and Migrate to MySQL","Questions"],
                           default_index=0,
                           orientation="vertical")

# Connent to MySQL database
api_service_name = "youtube"
api_version = "v3"
Api_key = "AIzaSyDYOhyvfSsKVivdTyHhuqzNzkbzB_QZQzM" 
youtube = build(api_service_name, api_version, developerKey = Api_key)

mysql = sql.connect(host = "localhost",user= "root",password = "mysqlroot",database ="youtube")
cursor = mysql.cursor()

# HOME PAGE
if selected == "Home":
    st.title(':red[YouTube Data Harvesting and Warehousing using SQL and Streamlit]')
    st.subheader(':blue[Domain:] Social Media')
    st.subheader(':blue[Overview:] Build a Streamlit app where users can enter a YouTube channel ID to view and select channel details for data migration. Use the YouTube API to fetch data and temporarily store it in pandas DataFrames. Migrate the cleaned data to a SQL database like MySQL or PostgreSQL. Use SQL to query this database based on user input. Finally, display the results in Streamlit using charts and graphs for easy analysis.')
    st.subheader(':blue[Skills Take Away:] Python Scripting, Data Collection, API integration, Data Management using SQL, Streamlit')


                
# Extract and Migrate to MySQL
if selected == " Extract and Migrate to MySQL":
    st.title(':red[Extract and Migrate to MySQL]')
    searchbox = st.text_input('Channel id:')
    def onSearchClick():
         if searchbox:
            request = youtube.channels().list(
            part="snippet,ContentDetails,statistics",
            id=searchbox
            )
            response = request.execute()

            data = {
            'channel_name': response['items'][0]['snippet']['title'],
            'channel_id' :searchbox,
            'channel_totalvideos': response['items'][0]['statistics']['videoCount'],
            'channel_description': response['items'][0]['snippet']['description'],
            'channel_playlists': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            'channel_viewCount': response['items'][0]['statistics']['viewCount'],
            'channale_sub': response['items'][0]['statistics']['subscriberCount']
                }
            query = 'insert into youtube.channels (channel_name, channel_id, channel_totalvideos, channel_description, channel_playlists, channel_viewCount, channale_sub) values (%s,%s,%s,%s,%s,%s,%s)'
            values = tuple(data.values())
            cursor.execute(query, values)
            mysql.commit()
            return data
        

    if st.button("Search"):
            
            data = onSearchClick()
            st.write("Channel_Name : "+data['channel_name']+' \n\r '+
        "Channel Id: "+data['channel_id']+' \n\r '+
        "Channel_Totalvideo : "+str(data ['channel_totalvideos'])+' \n\r ' +
        "Channel_Description: "+data ['channel_description']+' \n\r '+
        "Channel_Playlists : "+data ['channel_playlists']+' \n\r '+
        "Channel_Views: "+str(data ['channel_viewCount'])+' \n\r '+
        "Subscription_Count: "+str(data ['channale_sub'])+' \n\r '
    
        )


    def getVideosIds():
        if searchbox:
            video_ids = []
            response = youtube.channels().list(id = searchbox,
                                    part = 'contentDetails').execute()
            playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            token = None
            while True:
                request = youtube.playlistItems().list(
                part="contentDetails",
                maxResults=50,
                pageToken = token,
                playlistId=playlist_id
                )
                response = request.execute()
                if 'items' in response:
                    y = response['items']
                    for i in y:
                        video_id = i['contentDetails']['videoId']
                        video_ids.append(video_id)
                else:
                    break
                token = response.get("nextPageToken")
                if not token:
                    break
            return video_ids
                

    def video_details(video_id):
        if searchbox:
            vid_stat = [] 
            request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
            response = request.execute()
            for i in response['items']:
                z= dict(
                    channel_id = i['snippet']['channelId'],
                    channel_name = i['snippet']['channelTitle'] ,
                    video_id=i['id'],
                    video_name=i['snippet']['title'],
                    video_description=i['snippet']['description'],
                    published_date=i['snippet']['publishedAt'],
                    view_count=i['statistics'].get('viewCount'),
                    comment_count=i['statistics'].get('commentCount'),
                    duration=i['contentDetails']['duration'],
                    like_count = i['statistics'].get('likeCount')
                )

                vid_stat.append(z)

            return vid_stat
    query11 = "SELECT channel_name, video_id, duration FROM youtube.video_details order by duration;"
    res = cursor.execute(query11)
    myresult = cursor.fetchall()
    
    def getDurationInSec(duration):
        if duration == 'P0D':
            return 0
        
        duration = duration.replace('PT', '')
        hours, minutes, seconds = 0, 0, 0
        
        if 'H' in duration:
            hours, duration = duration.split('H')
            hours = int(hours)
        
        if 'M' in duration:
            minutes, duration = duration.split('M')
            minutes = int(minutes)
        
        if 'S' in duration:
            seconds = int(duration.split('S')[0])
        
        return hours * 3600 + minutes * 60 + seconds


    for result in myresult:
        queryUpdate = "UPDATE youtube.video_details SET duration_sec = %s where video_id=%s;"
        values = (getDurationInSec(result[2]),result[1])
        cursor.execute(queryUpdate, values)
        mysql.commit()    


    def get_comment_info(video_ids):
        if searchbox:
            comment_data = []
            
            try:
                for video_id in video_ids:
                    request = youtube.commentThreads().list(
                            part="snippet",
                            maxResults=50,
                            videoId=video_id
                    )
                    response = request.execute()
                    for item in response['items']:
                        data = dict(Comment_Id = item ['snippet']['topLevelComment']['id'],
                                video_id = item ['snippet']['topLevelComment']['snippet']['videoId'],
                                comment_text = item ['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author = item ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                comment_published = item ['snippet']['topLevelComment']['snippet']['publishedAt'])
                        query = "insert into youtube.comments (Comment_Id, video_id, comment_text, comment_author, comment_published) values (%s,%s,%s,%s,%s)"
                        values = tuple(data.values())
                        cursor.execute(query,values)
                        mysql.commit()
                        comment_data.append(data)
                return comment_data 
            except:
                pass     
                
        
    def get_playlist_info(channel_id):
        playlist_details = []
        request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults = 50
            )
        response = request.execute()
        for item in response['items']:
            data = dict(playlist_Id = item ['id'],
                        Title = item ['snippet']['title'],
                        channel_Id = item ['snippet']['channelId'],
                        channel_Name = item ['snippet']['channelTitle'],
                        published = item ['snippet']['publishedAt'],
                        video_count = item ['contentDetails']['itemCount']
            )
            query = 'insert into youtube.playlist (playlist_Id, Title, channel_Id, channel_Name, published, video_count) values (%s,%s,%s,%s,%s,%s)'
            values = tuple(data.values())
            cursor.execute(query, values)
            mysql.commit()
            playlist_details.append(data)
        return playlist_details
    
    
    if st.button("Migrate"):
            
            data = getVideosIds()
            data
            vdoDetails = []
            for vdoId in data:
                info = video_details(vdoId)
                query = 'insert into youtube.video_details (channel_id, channel_name, video_id, video_name, video_description, published_date, view_count,comment_count, duration, like_count) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                values = tuple(info[0].values())
                cursor.execute(query, values)
                mysql.commit()
                vdoDetails.append(info)     
            vdoDetails
            comments = get_comment_info(data)
            comments
            playlist = get_playlist_info(searchbox)
            playlist

            

if selected =="Questions":
    st.title(':red[Questions]')
    st.markdown(":red[Select any one of the options]")
    Questions = st.selectbox("Select the Questions",
                           ('1. What are the names of all the videos and their corresponding channels?',
                           '2. Which channels have the most number of videos, and how many videos do they have?',
                           '3. What are the top 10 most viewed videos and their respective channels?',
                           '4. How many comments were made on each video, and what are their corresponding video names?',
                           '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                           '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                           '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                           '8. What are the names of all the channels that have published videos in the year 2022',
                           '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                           '10. Which videos have the highest number of comments, and what are their corresponding channel names?',)) 


    if Questions=="1. What are the names of all the videos and their corresponding channels?":
        cursor.execute("SELECT channel_name, video_name FROM youtube.video_details;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)


    elif Questions =="2. Which channels have the most number of videos, and how many videos do they have?":
        cursor.execute("SELECT channel_name, channel_totalvideos FROM youtube.channels order by channel_totalvideos DESC;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)
        
    
    elif Questions=="3. What are the top 10 most viewed videos and their respective channels?":
        cursor.execute("SELECT view_count, video_name ,channel_name FROM youtube.video_details order by view_count desc limit 10;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)


    elif Questions=="4. How many comments were made on each video, and what are their corresponding video names?":
        cursor.execute("SELECT video_name,  comment_count FROM youtube.video_details order by comment_count desc;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)


    elif Questions=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        cursor.execute("SELECT channel_name, video_id, video_name, like_count FROM youtube.video_details order by like_count DESC;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)


    elif Questions=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        cursor.execute("SELECT video_name, like_count FROM youtube.video_details order by like_count DESC;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)


    elif Questions=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
        cursor.execute("SELECT channel_name, channel_viewCount FROM youtube.channels order by channel_viewCount DESC;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)


    elif Questions=="8. What are the names of all the channels that have published videos in the year 2022":
        cursor.execute("SELECT channel_name, published_date FROM youtube.video_details  where published_date = 2022;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)


    elif Questions=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        cursor.execute("SELECT avg(duration_sec),channel_name FROM youtube.video_details group by channel_name;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)


    elif Questions=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        cursor.execute("SELECT channel_name,video_name, comment_count FROM youtube.video_details order by comment_count DESC;")
        df = pd.DataFrame(cursor.fetchall(),columns = cursor.column_names)
        st.write(df)
