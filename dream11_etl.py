# ETL pipeline (Extract - Transform - Load) using AWS RDs

# Extract: fetching data
# Transform - Data cleaning
# Load - uploading back to a new dbs


# we have ball by ball data on aws server
# we have to extract that data, transform it in format "how many dream11 points earned by each player in each match"
# load transformed data back to aws server which will be used by DS team to create "Predictor Model"


# OLTP: Online Transactional Dbs
# OLAP: Online Analytical Dbs
# Any company will have a running dbs which users interact with regularly, thats why any analysis shudnt be done on OLTP bcoz website or server may collapse.
# so we create a dbs "OLAP" which can be used by analytical team by ETL. we'll extract OLTP dbs, Transform & Load it back to server as OLAP which will be used by DS team.


# so we setup a pipeline between OLTP & OLAP, so that as soon as new data comes
# ETL will be performed automatically, it will uploaded on OLAP dbs


# steps
# extract data from aws dbs
# transform data|
# create new dbs
# upload transformed data on new dbs


# import mysql.connector
# conn = mysql.connector.connect(host='',user = '',password = '',database = '')

# import pandas as pd
# delivery = pd.reqd_sql_query('SELECT * FROM delivery',conn)
# player = pd.reqd_sql_query('SELECT * FROM player',conn)
# player_cap = pd.reqd_sql_query('SELECT * FROM player_cap',conn)


# player = pd.read_csv('/content/Player.csv')
# player.head()

# player_captain = pd.read_csv('/content/Player_Match.csv')
# player_captain.head()

# delivery = pd.read_csv('/content/ipl_deliveries.csv')
# delivery.head()


# Step2 - Transform
# merging player & player captain to find out whether player was captain or not

temp_df = player.merge(player_captain,on='Player_Id')[['Player_Name','Match_Id','Is_Captain']]
temp_df


delivery = delivery.merge(temp_df,left_on = ['ID','batter'],right_on = ['Match_Id','Player_Name'],how='left').fillna(0)
delivery.head()

runs = delivery.groupby(['ID','batter'])['batsman_run'].sum().reset_index()
runs.rename(columns={'batsman_run':'total_runs'},inplace=True)
runs

balls = delivery.groupby(['ID','batter'])['batsman_run'].count().reset_index()
balls.rename(columns={'batsman_run':'balls_played'},inplace=True)
balls

fours = delivery.query('batsman_run == 4').groupby(['ID','batter'])['batsman_run'].count().reset_index()
fours.rename(columns = {'batsman_run':'fours'},inplace=True)
fours.head()

sixes = delivery.query('batsman_run == 6').groupby(['ID','batter'])['batsman_run'].count().reset_index()
sixes.rename(columns = {'batsman_run':'sixes'},inplace=True)
sixes

runs.merge(balls,on=['ID','batter'])

final_df = runs.merge(balls,on=['ID','batter']).merge(fours,on=['ID','batter']).merge(sixes,on=['ID','batter'])
final_df

final_df.fillna(0,inplace=True)

final_df.sample(5)

final_df['sr'] = round((final_df['total_runs'] / final_df['balls_played'])*100,2)

# final_df.drop(columns=['balls_played'],inplace=True)
# final_df.head()

final_df = final_df.merge(temp_df,left_on=['ID','batter'],right_on = ['Match_Id','Player_Name'],how='left').drop(columns = ['Player_Name','Match_Id'])

final_df.fillna(0,inplace=True)

final_df['Is_Captain'].value_counts()

#so we have extracted data frm co. dbs & transformed it into below format, now we have to add scoring based on dream11

final_df.sample(5)

# Now add score based on batting performance
def dream11(row):
  score = 0
  score += row['total_runs'] + row['fours'] + 2 * (row['sixes'])

  if row['total_runs'] >= 30 and row['total_runs'] < 50:
    score += 4

  elif row['total_runs'] >= 50 and row['total_runs'] < 100:
    score += 8

  elif row['total_runs'] >= 100:
    score += 16

  elif row['total_runs'] == 0:
    score -= 2

  if row['balls_played'] >= 10:
    if row['sr'] > 170:
      score += 6

    elif row['sr'] > 150 and row['sr'] <= 170:
      score += 4

    elif row['sr'] > 130 and row['sr'] <= 150:
      score += 2

    elif row['sr'] > 60 and row['sr'] <= 70:
      score -= 2

    elif row['sr'] > 50 and row['sr'] <= 59.99:
      score -= 4

    elif row['sr'] < 50:
      score -= 6            

    else:
      pass

  if row['Is_Captain'] == 1:
    score *= 2

  return score



final_df['score'] = final_df.apply(dream11,axis=1)

final_df.sample(5)

final_df.sort_values('score',ascending=False)



# Load
# final_df is transformed the way we wanted, so we will load this to the Dbs



export_df = final_df[['ID','batter','score']]
export_df

#loading this data to aws dbs



# go to aws account --> my account --> 'aws mgmt console' --> click on 'services' --> click on 'database'
# click on RDS (relational dbs services)

# 'create Dbs'
# choose Dbs creation method --> standard
# engine type --> mysql
# templates --> free tier

# provide Dbs name in 'Settings' : database-olap
# provide master username = username
# provide password = password

# Enable storage scaling = disable it (for safer side)
# select: dont connect to EC2 compute resource
# public access = Yes
# create Dbs



# click on Dbs & under security click on "vpc security groups"
# inbound rules --> "edit inbound rules" --> add 2 rules

# all traffic + anywhere ipv4 
# all traffic + anywhere ipv6 --> save rules



# connectivity & security --> copy endpoint --> paste it under host
# enter username & password which you created
# no dbs created, Its just a Dbs server


# connect to the Dbs
# conn = mysql.connector.connect(host='',user = '',password = '')


# upload export_df to Dbs server & create a new Dbs

# import pymysql
# from sqlalchemy import create_engine


# create Dbs

# mycursor = conn.cursor()
# mycursor.execute('CREATE DATABASE dream11)
# conn.commit()


# engine = create_engine('mysql+pymysql://user:password@url/database')
# {root}:{password}@{url}/{database}
# df.to_sql('table_name',con = engine)

# update the values in username, password, url, database name, table_name


# so we have completed all 3 steps of ETL, now DS team will fetch the data from dbs & create ml model on it

