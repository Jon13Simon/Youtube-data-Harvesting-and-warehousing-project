from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API key connection 

def Api_connect():
    Api_id= 'AIzaSyAtVMtQ1WH3nej0TtKtKFHFnG4iB73zcBE'
    Api_service_name= "Youtube"
    Api_version= 'v3'

    youtube= build(Api_service_name, Api_version, developerKey= Api_id)

    return youtube

youtube= Api_connect()
    

# get channel information

def get_channel_info(channel_id):
    request= youtube.channels().list(
        part = 'snippet,ContentDetails,statistics',
        id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data= dict(Channel_name=i['snippet']['title'],
               Channel_Id=i['id'],
               Subscribers=i['statistics']['subscriberCount'],
               Views=i['statistics']['viewCount'],
               Total_Videos=i['statistics']['videoCount'],
               Channel_Description=i['snippet']['description'],
               Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data


#get video Id

def get_video_ids(channel_id):
    video_ids=[]
    response= youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1= youtube.playlistItems().list(
            part ='snippet',
            playlistId= Playlist_Id,
            maxResults=50,
            pageToken= next_page_token).execute()
            

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token= response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# get video information

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request= youtube.videos().list(
            part= 'snippet, ContentDetails, statistics',
            id= video_id
        )
        response= request.execute()

        for item in response['items']:
            data= dict(Channel_Name= item['snippet']['channelTitle'],
                    Video_Id= item['id'],
                    Video_name= item['snippet']['title'],
                    Channel_Id= item['snippet']['channelId'],
                    Video_desc= item['snippet'].get('description'),
                    Tags= item['snippet'].get('tags'),
                    Published_at= item['snippet']['publishedAt'],
                    View_count= item['statistics'].get('viewCount'),
                    Like_count= item['statistics'].get('likeCount'),
                    Favorite_count= item['statistics']['favoriteCount'],
                    Comment_count= item['statistics'].get('commentCount'),
                    Duration= item['contentDetails']['duration'],
                    Thumbnails= item['snippet']['thumbnails']['default']['url'],
                    Caption_Status= item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data


# get comment information

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request= youtube.commentThreads().list(
                part= 'snippet',
                videoId= video_id,
                maxResults= 50
            )

            response= request.execute()

            for item in response['items']:
                data= dict(Comment_id= item['snippet']['topLevelComment']['id'],
                        Video_Id= item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text= item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Author_name= item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Publish_at= item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass
    return Comment_data

# upload to mongoDB

client= pymongo.MongoClient('mongodb+srv://Jonsimon:Jonsimon@cluster0.mex5jua.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db= client['Youtube_data']


def channel_details(channel_id):
    ch_details= get_channel_info(channel_id)
    vi_ids= get_video_ids(channel_id)
    vi_details= get_video_info(vi_ids)
    com_details= get_comment_info(vi_ids)

    coll= db['channel_details']
    coll.insert_one({'channel_information':ch_details,'video_information':vi_details, 'comment_information':com_details})

    return 'upload completed succesfully'

#Table creation for channels,videos,comments

def channels_table(channel_name_s):
    mydb= psycopg2.connect(host= 'localhost',
                        user= 'postgres',
                        password= 'Joshie@0910',
                        database= 'youtube_data',
                        port= '5432')
    cursor= mydb.cursor()

    
    create_query= '''create table if not exists channels(Channel_name varchar(100),
                                                        Channel_Id varchar (80) primary key,
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_Videos bigint,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
    
    cursor.execute(create_query)
    mydb.commit()

    single_channel_detail=[]
    db= client['Youtube_data']
    coll= db['channel_details']

    for ch_data in coll.find({'channel_information.Channel_name': channel_name_s},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])

    df_single_channel_detail= pd.DataFrame(single_channel_detail)


    for index,row in df_single_channel_detail.iterrows():
        insert_query= '''insert into channels(Channel_name,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])

        try:

            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            news =f"Your channel name {channel_name_s} is alreaded added to database"
            return news

# video table

def videos_table(channel_name_s):
    mydb= psycopg2.connect(host= 'localhost',
                        user= 'postgres',
                        password= 'Joshie@0910',
                        database= 'youtube_data',
                        port= '5432')
    cursor= mydb.cursor()


    create_query= '''create table if not exists videos(Channel_Name varchar(100),
                        Video_Id varchar(40) primary key,
                        Video_name varchar(150),
                        Channel_Id varchar(100),
                        Video_desc text,
                        Tags text,
                        Published_at timestamp,
                        View_count bigint,
                        Like_count bigint,
                        Favorite_count bigint,
                        Comment_count int,
                        Duration interval,
                        Thumbnails varchar(200),
                        Caption_Status varchar(50))'''
    cursor.execute(create_query)
    mydb.commit()

    single_video_detail=[]
    db= client['Youtube_data']
    coll= db['channel_details']

    for ch_data in coll.find({'channel_information.Channel_name': channel_name_s},{"_id":0}):
        single_video_detail.append(ch_data["video_information"])

    df_single_video_detail= pd.DataFrame(single_video_detail[0])
    
    for index,row in df_single_video_detail.iterrows():
            insert_query= '''insert into videos(Channel_Name,
                        Video_Id ,
                        Video_name,
                        Channel_Id,
                        Video_desc,
                        Tags,
                        Published_at,
                        View_count,
                        Like_count,
                        Favorite_count,
                        Comment_count,
                        Duration,
                        Thumbnails,
                        Caption_Status)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            
            values=(row['Channel_Name'],
                    row['Video_Id'],
                    row['Video_name'],
                    row['Channel_Id'],
                    row['Video_desc'],
                    row['Tags'],
                    row['Published_at'],
                    row['View_count'],
                    row['Like_count'],
                    row['Favorite_count'],
                    row['Comment_count'],
                    row['Duration'],
                    row['Thumbnails'],
                    row['Caption_Status'])
            
            
            cursor.execute(insert_query,values)
            mydb.commit()
    
    

def comments_table(channel_name_s):

    mydb= psycopg2.connect(host= 'localhost',
                        user= 'postgres',
                        password= 'Joshie@0910',
                        database= 'youtube_data',
                        port= '5432')
    cursor= mydb.cursor()


    create_query= '''create table if not exists comments(Comment_id varchar(50) primary key,
                                                            Video_Id varchar(50),
                                                            Comment_Text text,
                                                            Author_name varchar(50),
                                                            Publish_at timestamp)'''
    
    cursor.execute(create_query)
    mydb.commit()

    single_comments_detail=[]
    db= client['Youtube_data']
    coll= db['channel_details']

    for ch_data in coll.find({'channel_information.Channel_name': channel_name_s},{"_id":0}):
        single_comments_detail.append(ch_data["comment_information"])

    df_single_comments_detail= pd.DataFrame(single_comments_detail[0])

    for index,row in df_single_comments_detail.iterrows():
            insert_query= '''insert into comments(Comment_id,
                                                Video_Id,
                                                Comment_Text,
                                                Author_name,
                                                Publish_at)
                                            
                                            values(%s,%s,%s,%s,%s)'''
            
            values=(row['Comment_id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Author_name'],
                    row['Publish_at']
                    )
            
            
            cursor.execute(insert_query,values)
            mydb.commit()


def tables(single_channel):
    news= channels_table(single_channel)

    if news:
        return news
    
    else:
        videos_table(single_channel)
        comments_table(single_channel)

    return 'Tables created successfully'


def show_channels_table():
    ch_list=[]
    db= client['Youtube_data']
    coll= db['channel_details']

    for ch_data in coll.find({},{'_id':0,'channel_information':1}):
        ch_list.append(ch_data['channel_information'])

    df= st.dataframe(ch_list)

    return df

def show_videos_tables():
    vi_list=[]
    db= client['Youtube_data']
    coll= db['channel_details']

    for vi_data in coll.find({},{'_id':0,'video_information':1}):
        for i in range(len(vi_data['video_information'])):
            vi_list.append(vi_data['video_information'][i]
        )

    df2= st.dataframe(vi_list)

    return df2

def show_comments_tables():
    com_list=[]
    db= client['Youtube_data']
    coll= db['channel_details']

    for com_data in coll.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i]
        )

    df3= st.dataframe(com_list)

    return df3

# streamlit part

with st.sidebar:
    st.title(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]')
    st.header('Skill Take Away')
    st.caption('Python Scripting')
    st.caption('Data Collection')
    st.caption('MongoDB')
    st.caption('API Integration')
    st.caption('Data Management Using MongoDB and SQL')

channel_id= st.text_input('Enter the channel ID')

if st.button('collect and store data'):
    ch_ids=[]
    db.client['Youtube_data']
    coll=db['channel_details']
    for ch_data in coll.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])

    if channel_id in ch_ids:
        st.success('Channel Details of the given channel id already exists')

    else:
        insert= channel_details(channel_id)
        st.success(insert)

all_channels=[]
db= client['Youtube_data']
coll= db['channel_details']

for ch_data in coll.find({},{'_id':0,'channel_information':1}):
    all_channels.append(ch_data['channel_information']['Channel_name'])

unique_channel=st.selectbox("Select the channel",all_channels)

if st.button('Migrate to Sql'):
    Table= tables(unique_channel)
    st.success(Table)

show_table= st.radio('SELECT THE TABLE FOR VIEW',('CHANNELS','VIDEOS','COMMENTS'))

if show_table=='CHANNELS':
    show_channels_table()

elif show_table=='VIDEOS':
    show_videos_tables()

elif show_table=='COMMENTS':
    show_comments_tables()


# SQL connection

mydb= psycopg2.connect(host= 'localhost',
                    user= 'postgres',
                    password= 'Joshie@0910',
                    database= 'youtube_data',
                    port= '5432')
cursor= mydb.cursor()

question= st.selectbox('Select your question',('1. What are the names of all videos and channel?',
                                               '2. Which channels have the most number of videos?',
                                               '3. What are the top 10 most viewed videos?',
                                               '4. How many comments were made on each video?',
                                               '5. Which videos have the highest number of likes?',
                                               '6. What is the total number of likes for each video?',
                                               '7. What is the total number of views for each channel?',
                                               '8. What are the names of all the channels that have published videos in the year 2022?',
                                               '9. What is the average duration of all videos in each channel?',
                                               '10. Which videos have the highest number of comments?'))

if question=='1. What are the names of all videos and channel?':
    query1='''select video_name as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1= cursor.fetchall()
    df= pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=='2. Which channels have the most number of videos':
    query2='''select channel_name as channelname,total_videos as no_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2= cursor.fetchall()
    df2= pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=='3. What are the top 10 most viewed videos?':
    query3='''select view_count as views,channel_name as channelname,video_name as videotitle from videos
                where view_count is not null order by view_count desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3= cursor.fetchall()
    df3= pd.DataFrame(t3,columns=["views","channel name","video title"])
    st.write(df3)

elif question=='4. How many comments were made on each video?':
    query4='''select comment_count as no_comments,video_name as videotitle 
                from videos where comment_count is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4= cursor.fetchall()
    df4= pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=='5. Which videos have the highest number of likes?':
    query5='''select video_name as videotitle,channel_name as channelname, like_count as likecount
                from videos where like_count is not null order by like_count desc'''
    cursor.execute(query5)
    mydb.commit()
    t5= cursor.fetchall()
    df5= pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=='6. What is the total number of likes for each video?':
    query6='''select like_count as likes,video_name as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6= cursor.fetchall()
    df6= pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=='7. What is the total number of views for each channel?':
    query7='''select channel_name as channelname,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7= cursor.fetchall()
    df7= pd.DataFrame(t7,columns=["channelname","totalviews"])
    st.write(df7)

elif question=='8. What are the names of all the channels that have published videos in the year 2022?':
    query8='''select video_name as video_title, published_at as videorelease,channel_name as channelname from videos
                where extract(year from published_at)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8= cursor.fetchall()
    df8= pd.DataFrame(t8,columns=["videotitle","videorelease","channelname"])
    st.write(df8)

elif question=='9. What is the average duration of all videos in each channel?':
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos
                group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9= cursor.fetchall()
    df9= pd.DataFrame(t9,columns=["channelname","averageduration"])
    st.write(df9)

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row['channelname']
        average_duration=row['averageduration']
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)

elif question=='10. Which videos have the highest number of comments?':
    query10='''select video_name as videotitle, channel_name as channelname, comment_count as comments from videos
            where comment_count is not null order by comment_count desc'''
    cursor.execute(query10)
    mydb.commit()
    t10= cursor.fetchall()
    df10= pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)



