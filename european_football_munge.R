library(RSQLite)
library(dplyr)

con = dbConnect(drv=SQLite(), dbname="database.sqlite")
alltables = dbListTables(con)
alltables

dbListFields(con, "Player")
#list fields of player category
player_data <- dbGetQuery(con, "select * from  Player")
#list all attributes of the Player DF

select(player_data, player_api_id, player_name)
#select just the player id and the player name

player_info_df <- select(player_data, player_api_id, player_name)
#create df with the player id and player name

player_attributes_data <- dbGetQuery(con, "select * from  Player_Attributes")
#huge data frame with all list of player attributes

player_ratingandname_df <- select(player_attributes_data, player_api_id, overall_rating)
#df of player id and overall rating

df.agg <- aggregate(overall_rating ~ player_api_id, player_ratingandname_df, max)
#get the highest rating of each player id

name_id_rating_df <- left_join(player_info_df,df.agg, by = "player_api_id")
#df with name, id, and their overall ratings

gk_values_df <- select(player_attributes_data, player_api_id, gk_diving, gk_handling, gk_kicking, gk_positioning, gk_reflexes)
#data frame of the 5 values that make up gk rating, with their player id's

#gk_rating_df <- rowSums(gk_values_df[,c('gk_diving', 'gk_handling', 'gk_kicking', 'gk_positioning', 'gk_reflexes')], na.rm=TRUE)
gk_values_df <- mutate(gk_values_df, gk_rating = gk_diving + gk_handling + gk_kicking + gk_positioning + gk_reflexes)
#create the gk_rating by summing up the 5 separate gk values

df.highest_rating <- aggregate(gk_rating ~ player_api_id, gk_values_df, max)
#isolate the duplicates of the gk_rating's that you get

name_id_overallrating_gk_rating_df <- left_join(name_id_rating_df, df.highest_rating, by = "player_api_id")
#df with the name, id, overall_rating, and gk_rating

dbListFields(con, "Match")
match_data <- dbGetQuery(con, "select * from  Match")
#get all data for the matches

dbListFields(con, "Team")
team_data <- dbGetQuery(con, "select * from  Team")
#get all the data for the teams

team_idandname_df <- select(team_data, team_api_id, team_long_name)
#data frame with team id and their long names

dbListFields(con, "Team_Attributes")
team_attributes_data <- dbGetQuery(con, "select * from  Team_Attributes")
#get all the data for the team attributes

buildupplay_values_df <- select(team_attributes_data, team_api_id, buildUpPlaySpeed, buildUpPlayDribbling, buildUpPlayPassing)
#data frame with the team id's and their build up values

buildupplay_values_df1 <- mutate(buildupplay_values_df, buildUpPlay = buildUpPlaySpeed + buildUpPlayDribbling + buildUpPlayPassing)
#data frame with the build up play values and the new column for total buildupplay

buildUpPlay_df_final <- select(buildupplay_values_df1, team_api_id, buildUpPlay)
buildUpPlay_df_final <- na.omit(buildUpPlay_df_final)
#isolate just the team api id with the buildupplay values and omit NA's

buildUpPlay_df_final1 <- aggregate(buildUpPlay ~ team_api_id, buildUpPlay_df_final, max)
id_team_buildupplay_df <- left_join(team_idandname_df, buildUpPlay_df_final1, by = "team_api_id")
#create new df with the id, long team name, and the buildupplay

chancecreation_value_df <- select(team_attributes_data, team_api_id, chanceCreationPassing, chanceCreationCrossing, chanceCreationShooting)
chancecreation_value_df1 <- mutate(chancecreation_value_df, chanceCreation = chanceCreationPassing + chanceCreationCrossing + chanceCreationShooting)
chancecreation_df_final <- select(chancecreation_value_df1, team_api_id, chanceCreation)
chancecreation_df_final1 <- aggregate(chanceCreation ~ team_api_id, chancecreation_df_final, max)
#get just the team_api_id with the chance creation values. get the overall chance creation, then get the max values

id_team_buildupplay_chancecreation_df <- left_join(id_team_buildupplay_df, chancecreation_df_final1, by = "team_api_id")
#get the newest data frame with id, team, buildupplay, and chance creation

defence_value_df <- select(team_attributes_data, team_api_id, defencePressure, defenceAggression, defenceTeamWidth)
defence_value_df1 <- mutate(defence_value_df, defence = defencePressure + defenceAggression + defenceTeamWidth)
defence_df_final <- select(defence_value_df1, team_api_id, defence)
defence_df_final1 <- aggregate(defence ~ team_api_id, defence_df_final, max)
#get just the team_api_id with the defence variables. get overall defence, then get max values,isolate into df with 2 columns 

id_team_buildupplay_chancecreation_defence_df <- left_join(id_team_buildupplay_chancecreation_df, defence_df_final1, by = "team_api_id")
#get the newest data frame with id, team, buildupplay, and chance creation, and defence

homeandawayplayer_df <- select(match_data, home_player_1, home_player_2, home_player_3, home_player_4, home_player_5, home_player_6,
                               home_player_7, home_player_8, home_player_9, home_player_10, home_player_11, away_player_1, away_player_2, away_player_3,
                               away_player_4, away_player_5, away_player_6, away_player_7, away_player_8, away_player_9, away_player_10, away_player_11)
#create a data frame with the home_player_1 -> away_player_11

player_appearances_table <- table(unlist(homeandawayplayer_df))
player_appearances_df <- as.data.frame(player_appearances_table)
#get the number of appearances as a table, then change it into a data frame.

colnames(player_appearances_df)[1] <- "player_api_id"
colnames(player_appearances_df)[2] <- "numappearances"
#change the names of the columns so you can merge

name_id_overallrating_gk_rating_appearances_df <- merge(name_id_overallrating_gk_rating_df, player_appearances_df, by = "player_api_id")
#merge it all and get the name,id,overallrating,gkrating,and num appearances

write.csv(name_id_overallrating_gk_rating_appearances_df, "player.csv")
playercsv <- read.csv("player.csv")
print(playercsv)
#write it to a csv file for players' csv and read from the csv file
write.csv(id_team_buildupplay_chancecreation_defence_df, "team.csv")
teamcsv <- read.csv("team.csv")
print(teamcsv)
#write it to a csv for teams' csv and read from the file