# Input Necessary Libraries 
from googleapiclient.discovery import build
import streamlit as st
import pandas as pd
import mysql.connector

# Connent to MySQL database
conn = st.connection('mysql', type='sql')
api_service_name = "youtube"
api_version = "v3"
Api_key = "AIzaSyDYOhyvfSsKVivdTyHhuqzNzkbzB_QZQzM" 
youtube = build(api_service_name, api_version, developerKey = Api_key)

if 'flag' not in st.session_state:
    st.session_state['flag'] =False
if 'channelData' not in st.session_state:
    st.session_state['channelData'] = []
st.write("## Youtube Data Harvesting and Warehousing")
searchTxt = st.text_input('Channel id:')

# Streamlit app code 
def onSearchClick():
    st.session_state.flag = True
searchBtn = st.button("Search", on_click=onSearchClick)

channelIds= ['UCqA05jJsIlK5E0XH91fb2rg', 'UCeFsEjAnuLLN1J-ie6lO3jw', 'UCLnKZRT9orZAgttWtS2np2g', 
                 'UCrSit4ra9GmGH6Z0SczTATg', 'UCdEuF8BZZPtzxJnJmRqLKEw', 'UCcL78rRNuUQ8t7Dx4CLmRqA',
                 'UCymeXH2TJW58p5WcSeyDc3g', 'UCLYV9dSX1_KD46shbC7Hp4Q', 'UCrnDk_4-MwHR7HeUS-5BK8A', 
                 'UCr-gTfI7au9UaEjNCbnp_Nw']
data = ','.join(channelIds)
conn = st.connection('mysql', type='sql')
df = conn.query('SELECT * from channels')

response = []
for row in df.itertuples():
    eachChannel = {
        'channel_name': row.channel_name,
        'channel_id' :row.channel_id,
        'channel_totalvideos': row.channel_totalvideos,
        'channel_description': row.channel_description,
        'channel_publishedAt': row.channel_playlists,
        'channel_viewCount': row.channel_viewCount,
        'channale_sub': row.channale_sub
    }
    response.append(eachChannel)

def searchById(id):
    for index, channelDet in enumerate(response):
        if channelDet['channel_id'] == id:
            return channelDet
    return None

def onDetailsClick(channelId):
    query = "Select * from youtube.video_info where channel_id = :channelId"
    response = conn.query(query,params={"channelId":channelId})
    response

if st.session_state.flag == False:
    
    for index, value in enumerate(response):
        channel = value
        st.write("Channel_Name : "+channel['channel_name']+' \n\r '+
    "Channel Id: "+channel['channel_id']+' \n\r '+
    "Subscription_Count: "+str(channel ['channale_sub'])+' \n\r '+
    "Channel_Views: "+str(channel ['channel_viewCount'])+' \n\r '+
    "Channel_Description: "+channel ['channel_description']+' \n\r '+
    "Channel_Published : "+channel ['channel_publishedAt']+' \n\r '+
    "Channel_Totalvideo : "+str(channel ['channel_totalvideos'])+' \n\r '
   
    )
        query = "Select * from youtube.video_infos where channel_id = :channelId"
        response = conn.query(query,params={"channelId":channel['channel_id']})
        response
        st.write("---------------------------------------")

if st.session_state.flag == True:
    if searchTxt:
        request = youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=searchTxt
    )
        response = request.execute()
        data = {
            'channel_name': response['items'][0]['snippet']['title'],
            'channel_id' :searchTxt,
            'channel_totalvideos': response['items'][0]['statistics']['videoCount'],
            'channel_description': response['items'][0]['snippet']['description'],
            'channel_publishedAt': response['items'][0]['snippet']['publishedAt'],
            'channel_playlists': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            'channel_viewCount': response['items'][0]['statistics']['viewCount'],
            'channale_sub': response['items'][0]['statistics']['subscriberCount']
            }
    
        if data != None:

                st.write("Channel_Name : "+data['channel_name']+' \n\r '+
        "Channel Id: "+data['channel_id']+' \n\r '+
        "Subscription_Count: "+str(data ['channale_sub'])+' \n\r '+
        "Channel_Views: "+str(data ['channel_viewCount'])+' \n\r '+
        "Channel_Description: "+data ['channel_description']+' \n\r '+
        "Channel_Published : "+data ['channel_publishedAt']+' \n\r '+
        "Channel_Totalvideo : "+str(data ['channel_totalvideos'])+' \n\r '
    
        )
        print(data)

# FAQs 
st.write("""### FAQs""")
with st.expander("1. What are the names of all the videos and their corresponding channels?"):
    query = "SELECT channelTitle, title FROM youtube.video_infos;"
    response = conn.query(query)
    response
with st.expander("2. Which channels have the most number of videos, and how many videos do they have?"):
    query = "SELECT channel_name, channel_totalvideos FROM youtube.channels order by channel_totalvideos DESC;"
    response = conn.query(query)
    response
with st.expander("3. What are the top 10 most viewed videos and their respective channels?"):
    query = "SELECT viewCount, title,channelTitle FROM youtube.video_infos order by viewCount desc limit 10;"
    response = conn.query(query)
    response
with st.expander("4. How many comments were made on each video, and what are their corresponding video names?"):
    query = "SELECT title,  commentCount FROM youtube.video_infos order by commentCount desc;"
    response = conn.query(query)
    response
with st.expander("5. Which videos have the highest number of likes, and what are their corresponding channel names?"):
    query = "SELECT channelTitle, likeCount FROM youtube.video_infos order by likeCount DESC;"
    response = conn.query(query)
    response
with st.expander("6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?"):
    query = "SELECT title, likeCount FROM youtube.video_infos order by likeCount DESC;"
    response = conn.query(query)
    response
with st.expander("7. What is the total number of views for each channel, and what are their corresponding channel names?"):
    query = "SELECT channel_name, channel_viewCount FROM youtube.channels order by channel_viewCount DESC;"
    response = conn.query(query)
    response
with st.expander("8. What are the names of all the channels that have published videos in the year 2022?"):
    query = "SELECT channelTitle, publishedAt FROM youtube.video_infos  where publishedAt = 2022;"
    response = conn.query(query)
    response
with st.expander("9. What is the average duration of all videos in each channel, and what are their corresponding channel names?"):
    query = "SELECT avg(duration_sec),channelTitle FROM youtube.video_infos group by channelTitle;"
    response = conn.query(query)
    response
with st.expander("10. Which videos have the highest number of comments, and what are their corresponding channel names?"):
    query = "SELECT channelTitle,title, commentCount FROM youtube.video_infos order by commentCount DESC;"
    response = conn.query(query)
    response
