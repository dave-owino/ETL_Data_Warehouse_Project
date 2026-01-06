#!/usr/bin/env python
# coding: utf-8

# # Project: Investigate a Dataset - [Database_soccer]
# 
# ## Table of Contents
# <ul>
# <li><a href="#intro">Introduction</a></li>
# <li><a href="#wrangling">Data Wrangling</a></li>
# <li><a href="#eda">Exploratory Data Analysis</a></li>
# <li><a href="#conclusions">Conclusions</a></li>
# </ul>

# <a id='intro'></a>
# ## Introduction
# 
# ### Dataset Description 
# 
# > In this project 1, I willl be analysing ultimate soccer dataset, which is an open-source dataset in kaggle. The dataset is a one .sql file comprising seven tables, each with different(unique) but interrelated features. First, Country table has 11 European countries. Second, league table has 11 lead championship names. The country and league tables are related by their ID. Third, match table has over 25, 000 matches for different seasons as well as betting odds from upto 10 providers. The match table is also related to the previous tables by country_id. in the rows and 2 columns id and name
# I have check the shape of table to determine the nummber of rows and columns.
# 
# 
# ### Question(s) for Analysis
# 1. What teams improved the most over the time period? 
# 2. Which players had the most penalties? 
# 3. Which was the the most preferred leg for penalty-takers in 2016 among the players who scored more than the mean penalties in that year?

# In[1]:


# import statements for all of the packages to be used.

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

get_ipython().run_line_magic('matplotlib', 'inline')


# I want to create a function that would read csv and load for very dataset to a name variable

# In[2]:


# creating function to load data
def load_data(name, table_name):
    name=pd.read_csv('Database_Soccer/'+ table_name) # reads the csv file and stores in the dataframe name
    return name


# displaying few lines of each dataset from the soccer database

# In[3]:


country=load_data('country', 'Country.csv')# country data table
country.head()


# In[4]:


league=load_data('league', 'League.csv')# league data table
league.head()


# From the league name above, the league name for Germany is confusing. Germany 1. could mean there are a number of German countries, so it should be change to Germany Bundesliga 1

# In[5]:


match=load_data('match', 'Match.csv')# match data table
match.head()


# missing data in match table

# In[6]:


player=load_data('player', 'Player.csv')# player data table
player.head()


# The birthday contains time at 00:00:00, which could be removed to contain only year, month and date

# In[7]:


player_attr=load_data('player_attr', 'Player_Attributes.csv')# player attributes data table
player_attr.head()


# The time 00:00:00 can be removed

# In[8]:


team=load_data('team', 'Team.csv')# team data table
team.head()


# The column team_fifa_api_id could insignificant because there is already team_api_id

# In[9]:


team_attr=load_data('team_attr', 'Team_Attributes.csv')# team attributes data table
team_attr.head()


# I want again to develop a function that I will be using in inspecting the datasets for missing data, getting descriptive statistics, dimensions, and features' data types

# In[10]:


# creating function for inspecting the datasets
def wrangles (tbl_name):
    
    inf=tbl_name.info(); # inspecting data types and instances with missing data 
    dim=tbl_name.shape; # inspecting dimensions of the dataset
    desc=tbl_name.describe(); # getting descriptive statistics
        
    return inf, dim, desc


# In[11]:


# country dataset
wrangles(country)


# There is no need of getting the mean for ids, since the names are string we can just unique values and counts 

# In[12]:


country.name.describe()


# In[13]:


# league data
wrangles(league)


# There is no need of getting the mean for ids, since the names are string we can just unique values and counts

# In[14]:


league.name.describe()


# In[15]:


# match data
wrangles(match)


# missing data for column home_player_x1

# In[16]:


match.dtypes


# the date has been stored as a string

# In[17]:


# player data
wrangles(player)


# birthday column stored as string

# In[18]:


# player attributes data
wrangles(player_attr)


# dates stored as string

# In[19]:


# team data
wrangles(team)


# Missing data for team_fifa_api_id, though this might be insignificant since there is already team_api_id

# In[20]:


# team attributes data
wrangles(team_attr)


# Missing data for buildUpPlayDribbling

# 
# ### Data Cleaning
# 
# First, I want to create functions that will help me drop duplicates, merge two datasets, change data type, remove missing rows, drop unnecessary columns, then proceed to to merge the country data to that for league. 
# I will correct the league name for Germany 1. Bundesliga to Germany Bundesliga 1.
# I will also change the name column for both the country data and league data, and also make the datafrmaes have the same dimensions and finally merge the two dataframes into country_league data using the country id as the key

# In[21]:


# creating function to rename columns in a data frame
def col_rename(col_renamed_data, col_old, col_new):
    if len(col_old)==2: # checks if there are two columns to be renamed
        col_renamed_data.rename(columns={col_old[0]:col_new[0], col_old[1]:col_new[1]}, inplace=True) # renames the two columns in the dataset
    else:
        col_renamed_data.rename(columns={col_old:col_new}, inplace=True) # renames if there is only one column to be renamed
        
    return col_renamed_data


# In[22]:


# creating a function drop columns
def drop_cols(drop_col_data, col_name):
    drop_col_data.drop(col_name, axis=1, inplace=True) # removing columns
        
    return drop_col_data


# In[23]:


# creating a function to remove row missing values
def remove_row_missing_values(na_data):
    na_data.dropna(axis=0, how='any', inplace=True) # removing all rows with missing values
        
    return na_data


# In[24]:


# creating a function to remove duplicate rows
def remove_duplicates(dup_data, col):
    if col=='':
        dup_data.drop_duplicates(inplace=True) # remove all duplicate rows
        
    else:
        dup_data.dropna(subset=[col], inplace=True) # removing rows based on column duplicate values
        
    return dup_data


# In[25]:


# creating a function to change date column from string to datetime
def changed_type(changed_type_data, col_type):
    changed_type_data[col_type]=changed_type_data[col_type].astype('str')  # converting to string
    changed_type_data[col_type]=changed_type_data[col_type].str.extract(r'(\d{4}-\d{2}-\d{2})') # extracting the date
    changed_type_data[col_type] = pd.to_datetime(changed_type_data[col_type], format='%Y-%m-%d') # converting to datetime

    return changed_type_data


# In[26]:


# creating a function to filter some columns
def filter_col(f_data, col):
    df=f_data.filter(col) # filters the columns
        
    return df


# In[27]:


# creating a function to to merge two data frames using inner because i dont want do keep duplicates
def merging_data(data1, data2, on_col):
    df=data1.merge(data2, on =on_col, how='inner')
        
    return df


# In[28]:


# correcting the league name for Germany 1. Bundesliga
league.replace(to_replace='Germany 1. Bundesliga', value='Germany Bundesliga 1', inplace=True)


# In[29]:


# changing both id and name columns for the country data
country=col_rename(country, col_old=['name', 'id'], col_new=['country_name', 'country_id']) 
country 


# In[30]:


# changing the name column and id column  for the league data
league=col_rename(league, col_old=['name', 'id'], col_new=['league_name', 'league_id']) 
league


# In[31]:


# merging the two dataframes country and league
country_league_info=merging_data(country, league, on_col ='country_id')
country_league_info


# Second, I want to merge the player data to player attributes data into player_info dataframe.
# I will use either the player_api_id or player_fifa_api_id as the keys and drop the id columns in both datasets
# 

# In[32]:


# checking the number of unique values of player_api_id on payer and player attribute data
player.player_api_id.nunique()==player_attr.player_api_id.nunique()


# In[33]:


# checking the number of unique values of  player_fifa_api_id on player and player attribute data
player.player_fifa_api_id.nunique()==player_attr.player_fifa_api_id.nunique()


# Since the number of unique values of the player_fifa_api_id are not the same in both dataframes, I will just use both of them as the key because both the ids might be important in merging this data with another one

# In[34]:


# dropping id column from player dataframe
drop_cols(player, col_name='id')
player.head()


# In[35]:


# dropping id column from player_attr dataframe
drop_cols(player_attr, col_name='id')
player_attr.shape


# In[36]:


# removing duplicates for player attribute data
remove_duplicates(player_attr, col='')
player_attr.sort_values(['player_api_id', 'date']) # sorting the players by id and date


# In[37]:


# joining the two dataframes on player_api_id and player_fifa_api_id using inner join because i dont want the unmatched rows
player_info=merging_data(player, player_attr, on_col =['player_api_id', 'player_fifa_api_id'])
player_info


# I now want o examine the player_info dataframe, check for missing values, dimensions of each column, data types of each column as well as duplcate values

# In[38]:


player_info.info()


# From the above uotput, there are several missing values, the data type for birthday and date are all strings. I have also noted that attacking_work_rate has the least number of rows hence maximum number of missing values

# In[39]:


# getting the uniques for the coulmn attacking_work_rate
player_info.attacking_work_rate.unique()


# I want to drop all rows with nan and duplicate rows

# In[40]:


# dropping all rows with missing data
remove_row_missing_values(player_info) 


# In[41]:


# dropping duplicate rows
remove_duplicates(player_info, col='')


# In[42]:


player_info.info()


# In[43]:


player_info.attacking_work_rate.value_counts()


# In[44]:


player_info.defensive_work_rate.value_counts()


# From the output in above two cells, no information has been provided from the data description about the meaning of None, norm, y, stoc, le, ornal, es, tocky, ean o, and the numbers 0, 1, 2, 3, 4, 5, 6, 7, 8, and 9 . Howerver, from inspection, I realize that there is some association in that all the numbers 0-9 and o on the defensive_work_rate relates to None  on the attacking_work_rate. Also, the following  pairs also relate: norm-ornal, y-es, stoc-tocky, le-ean. These could be change or transformed if additional information is provided or simply be dropped from the analysis

# In[45]:


attacking_none=player_info.query('attacking_work_rate=="None"')
attacking_none


# In[46]:


# comparing length of numbers 0-9 and o in the defensive_work_rate relates to None in the attacking_work_rate

attacking_none.attacking_work_rate.value_counts()==attacking_none.defensive_work_rate.value_counts().sum()


# I want to change the data types for birthday and date from string to datetime

# In[47]:


# converting birthday column into datetime
changed_type(player_info, col_type='birthday')


# In[48]:


# converting date column into datetime
changed_type(player_info, col_type='date')


# In[49]:


player_info.info()


# Third, I want merge team data to team attributes data into into team info dataframe, check data types, missing values

# In[50]:


# checking the number of unique values of team_api_id in team and team attribute data
team.team_api_id.nunique()==team_attr.team_api_id.nunique()


# In[51]:


# checking the number of unique values of team_fifa_api_id in team and team attribute data
team.team_fifa_api_id.nunique()==team_attr.team_fifa_api_id.nunique()


# From these outputs, it is clear that neither the team_api_id nor the team_fifa_api_id matches in the two datasets. I will therefore merge them on both the team_api_id and team_fifa_api_id as the keys

# In[52]:


# dropping the id for team dataframe
drop_cols(team, col_name='id')


# In[53]:


team.team_long_name.unique()


# I want to correct the following team names: '1. FC Köln', '1. FC Nürnberg', '1. FSV Mainz 05','1. FC Kaiserslautern'

# In[54]:


# correcting some team names
team.team_long_name.replace(to_replace=['1. FC Köln', '1. FC Nürnberg', '1. FSV Mainz 05','1. FC Kaiserslautern'], 
                            value=['FC Köln', 'FC Nürnberg', 'FSV Mainz 05','FC Kaiserslautern'], inplace=True)


# In[55]:


# dropping the id for team attributes data
drop_cols(team_attr, col_name='id')


# In[56]:


# merging team to the team attribute
team_info=team.merge(team_attr, on =['team_api_id', 'team_fifa_api_id'], how='inner')
team_info


# I will populate the NaN in the buildUpPlayDribbling column with the mean of the column. And finally drop duplcates in the final dataframe

# In[57]:


# getting the mean of buildUpPlayDribbling column
mean=team_info.buildUpPlayDribbling.mean()

# filling the NaNs in the buildUpPlayDribbling by the mean
team_info['buildUpPlayDribbling']=team_info['buildUpPlayDribbling'].fillna(mean)


# In[58]:


# converting to datetime
changed_type(team_info, col_type='date')


# In[59]:


# dropping the duplicate rows in the final team info merged data
remove_duplicates(team_info, col='')


# In[60]:


team_info.info()


# In[61]:


team_info.head()


# Finally, I to examine and merge match data, country_league, and team data to form march info dataframe. I will check for duplicates, missing values and correct data types

# In[62]:


# checking the number of league_id unique values in the match and country_league_info dataframes
match.league_id.nunique()==country_league_info.league_id.nunique()


# In[63]:


# checking the number of country_id unique values in the match and country_league_info dataframes
match.country_id.nunique()==country_league_info.country_id.nunique()


# I will merge match and country_league_info dataframes on league_id and country_id, so that I retain all the info about ids. I want to drop all the columns containing odds

# In[64]:


for i in match.columns:
    print(i)


# I want to remove all the columns with CAPITAL LETTERS, odds columns

# In[65]:


# converting to datetime for the match data
changed_type(match, col_type='date')


# In[66]:


# dropping the duplicate rows from match data
remove_duplicates(match, col='')


# In[67]:


# dropping the columns containg odds and the match id
odd_cols = list(filter(lambda x: x.isupper(), match.columns))
drop_cols(match, col_name=odd_cols)
drop_cols(match, col_name='id')
match.tail()


# # checking the contents of some columns
# ```match.goal.value_counts() # goal```

# From the above two cells, I realised that the coulmns goal, shoton, shotoff, foulcommit, card, cross, corner and possession contains information related to the web page but not realistic data.Checking through the nested infomation, I can't really make sense out of it since even the website link to discription is not loading. Instead of deleting the columns with such issues, I will instead drop the columns containing unprocessed data

# In[68]:


match_info=filter_col(match, col=['country_id', 'league_id', 'season', 'stage', 'date', 'match_api_id', 'home_team_api_id', 'away_team_api_id', 'home_team_goal', 'away_team_goal'])

match_info.head()


# In[69]:


# melt home and away teams id_vars are columns not melted
# creating columns not melted
unmelted_columns= [x for x in match_info.columns if x not in ['home_team_api_id', 'away_team_api_id']]

# melting home and away teams api ids
melted_match=match_info.melt(id_vars=unmelted_columns, var_name='Location', value_name='Team')
melted_match.head()


# In[70]:


# cleaning up or changing the location values
melted_match['Location']=melted_match['Location'].replace({'home_team_api_id':'home', 'away_team_api_id':'away'})
melted_match.head()


# In[71]:


# creating team dictionary with team api ids as the key and a longname as the value
team_dict=team.set_index('team_api_id')['team_long_name'].to_dict()
team_dict


# In[72]:


# cleaning up team i.e. using team dictionary to replace team api ids
melted_match['Team']=melted_match['Team'].map(team_dict)
melted_match.head()


# In[73]:


# creating goals colum
melted_match['goals']=np.where(melted_match['Location']=='home',melted_match['home_team_goal'], melted_match['away_team_goal'])
melted_match.head()


# In[74]:


drop_cols(melted_match, col_name=['home_team_goal', 'away_team_goal'])
melted_match.head()


# In[75]:


melted_match.sort_values(by=['match_api_id', 'season'])
melted_match.head()


# <a id='eda'></a>
# ## Exploratory Data Analysis

# # creating a function to plot different visualizations

# In[76]:


def plot_visual(data, data2, visual_type):
    if visual_type=='barh':
        data.plot(kind='barh', rot=0, width=0.7, alpha=0.8, color='grey', figsize=[8,20] ) # i want to creat horizontal bars
            
    elif visual_type=='hist':
        fig,vis=plt.subplots(figsize=[10,8])
        vis.hist(data, alpha=0.8) # creating a histogram
        #plt.grid(axis='y', alpha=0.6) # grid
            
    elif visual_type=='boxplot':
        fig,vis=plt.subplots(figsize=[8,4])
        vis.boxplot(data, vert=0) # creating a box plot
        #plt.grid(axis='x', alpha=0.6)
                     
    else:
        fig, vis=plt.subplots(figsize=[10,8])
        plt.scatter(x=data, y=data2, alpha=0.8, color='blue') # creating scatter


# ### Research Question 1 What teams improved the most over the time period? 

# In[77]:


# checking the distribution of the extreme seasons
goal_08_16=melted_match.query('season in ["2008/2009", "2015/2016"]').groupby(['season', 'Team'])['goals'].sum()
# plot histogram
plot_visual(data=goal_08_16, data2='', visual_type='hist')
plt.title('SEASON 2008/2009 AND 2015/2016 HISTOGRAM')
plt.xlabel('bins')
plt.ylabel('No of goals scored in 2008/2009 and 2015/2015 season')


# The graph shows that the distribution of goals is right screwed. Further investigation can be shown on the boxplot

# In[78]:


# plot box plot
plot_visual(data=goal_08_16, data2='', visual_type='boxplot')
plt.title('SEASON 2008/2009 AND 2015/2016 BOXPLOT')
plt.xlabel('No of goals scored in 2008/2009 and 2015/2015 season')
plt.ylabel('2008/2009 and 2015/2015 season')


# The distribution is rght skewed but with severa outliers

# In[79]:


# grouping by season and team
match_s08_15=melted_match.query('season in ["2008/2009", "2015/2016"]').groupby(['season', 'Team'])['goals'].sum().unstack('season')
match_s08_15.head()


# In[80]:


# plot a scatter diagram
plot_visual(data=match_s08_15['2008/2009'], data2=match_s08_15['2015/2016'], visual_type='scatter')
plt.title('SCATTER PLOT FOR SEASON 2008/2016 VS 2015/2016')
plt.xlabel('Season 2008/2009')
plt.ylabel('Season 2015/2016')


# From the scatter plot, there is positive correlation between the number of goals scored in the season 2008/2009 and season 2015/2016

# In[81]:


# get difference between columns 2008/2009 and 2015/2016 seasons
goal_diff=match_s08_15.dropna().diff(axis=1)
goal_diff.head()


# In[82]:


# goal difference in 2015/2016 season
goal_diff_16=goal_diff['2015/2016']
goal_mean=goal_diff_16[goal_diff_16>(goal_diff_16.mean())].sort_values(ascending=True)


# In[83]:


# plot bar graph for top 10 best teams
plot_visual(data=goal_mean, data2='', visual_type='barh')
plt.title('SEASON 2008/2009 AND 2015/2016 BAR GRAPH')
plt.xlabel('No of goals scored in 2008/2009 and 2015/2015 season')
plt.ylabel('Teams');


# From the graph, the top 3 most improved teams are Paris Saint_Germain, Napoli and Cracovia in that order

# ### Research Question 2  Which players had the most penalties?

# I want to filter and obtain the player who scored most of the penalties

# In[84]:


# obtaining the name of the player
most_pen=player_info.query('penalties ==penalties.max()')

# creating dataframe for the player who had most penalties, avoiding duplicate name
most_pen.drop_duplicates(subset="player_api_id")

# printing 
most_pen


# The table shows that the player that scored most of the penalties is Richie Lambert

# ### Research Question 3  Which was the the most preferred leg for penalty-takers in 2016 among the players who scored more than the mean penalties in that year?

# Getting present in 2016

# In[85]:


most_pen16=player_info.query('date>="2016.01.01"')
most_pen16


# In[86]:


# get the players who had penalties more than the mean penalties
pen_mean=most_pen16[most_pen16.penalties>most_pen16.penalties.mean()]
pen_mean.drop(['date'], axis=1)


# In[87]:


# droping duplicates interms of player_api_id
pen_mean.drop_duplicates(subset=['player_api_id'])


# In[88]:


# creating a pie chart for the preffered leg
p=pen_mean['preferred_foot'].value_counts()
p.plot('pie', figsize=[6,6])
plt.title("PIE CHART FOR PREFfERRED LEG");


# From the pie chart, most penalty takers in the year 2016 preffered right leg than left

# <a id='conclusions'></a>
# ## Limitations
# The soccer database is a very extensive data. In, seeking to address the three questions in the description, I was able to show only the correltaion between the number of goals scored in the two season. However, the other seasons were not considered.The data base had a lot of unprocessed  html files under certain columns, which made take a lot of time thinking on how well they can be used in the analysis. In addition, the data had a lot of missing and duplicate values. Identifying such inconsistencies, wss realy time consuming.
# 
# Another limitation is that in the creation of bar graph for the most improved teams, the bars are not sorted in order of either increasing or decreasing frequency, which would have enabled the identification of the improved teams easily. In getting the most preffered leg, it was only based on the year 2016 instead of the whole duration of time.

# ## Conclusions
# The soccer database has five datasets, league, country, player, player attribute, team and team attribute. It is a detailed dabase for European major leagues covering several seasons from 2008/2009 t0 2015/2016. 
# 
# The project seeks to answer three questions, what teams improved the most over the time period, which players had the most penalties and which was the the most preferred leg for penalty-takers in 2016 among the players who scored more than the mean penalties in that year?
# 
# In attempting to find solutions to the question, each dataset was examineed for inconsistencies, colomn names, corrected, missing values replace or droped in certain datasets before they were finally merged and cleaned. Visual presentations created and inteprated.
# 
# From the analysis and visualization, Richie Lambert is the player who scored most of the penalties. I also found that Paris Saint-Germain is the most improved team over the period of time given, followed by Napoli and Cracovia being the in the third position. Moreover, the findings also indicate that most of the penalty takers in 2016 preferred right leg compared to the right leg. The findings also shows that the distribution of the number of goals scored in the two seasons are right skewed.
# 
# Whereas I was able to show that there is a correltaion between the number of goals scored in the two extreme seasons (2008/2009 and 2015/2016), theer are  other seasons that were not considered. There is likelihood that a team that improved between the two seasons might not have improved in the seasons prior 2015/2016. Goal difference between the two seasons was used as a measured of improvement in performance because the ultimate objective of team managers, players and teams is to improve to score goals, but there could be criteria for measuring performance.
