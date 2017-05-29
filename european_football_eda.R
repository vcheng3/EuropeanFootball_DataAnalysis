library(RSQLite)
library(dplyr)

playerdata <- read.csv("player.csv")
print(playerdata)
#read from the saved player file and print out contents

teamdata <- read.csv("team.csv")
print(teamdata)
#read from the saved team file and print out contents

overallranking_df <- select(playerdata, player_api_id,player_name, overall_rating)
overallranking_df1 <- arrange(overallranking_df, desc(overall_rating))
overallranking_df_final <- select(head(overallranking_df1, 10), player_name)
#get the top 10 by overall rating

gkrating_df <- select(playerdata, player_api_id,player_name, gk_rating)
gkrating_df1 <- arrange(gkrating_df, desc(gk_rating))
gkrating_df_final <-select(head(gkrating_df1,10), player_name)
#get the top 10 by gk rating

appearances_df <- select(playerdata, player_api_id, player_name, numappearances)
appearances_df1 <- arrange(appearances_df, desc(numappearances))
appearances_df_final <-select(head(appearances_df1,10), player_name)
#get the top 10 by number of appearances

buildupplay_df <- select(teamdata, team_api_id, team_long_name, buildUpPlay)
buildupplay_df1 <- arrange(buildupplay_df, desc(buildUpPlay))
buildupplay_df_final <- select(head(buildupplay_df1, 10), team_long_name)
#get the top 10 by number of buildupplay attributes

chancecreation_df <- select(teamdata, team_api_id, team_long_name, chanceCreation)
chancecreation_df1 <- arrange(chancecreation_df, desc(chanceCreation))
chancecreation_df_final <- select(head(chancecreation_df1, 10), team_long_name)
#get the top 10 by chanceCreation attributes

defence_df <- select(teamdata, team_api_id, team_long_name, defence)
defence_df1 <- arrange(defence_df, desc(defence))
defence_df_final <- select(head(defence_df1, 10), team_long_name)
#get the top 10 by chanceCreation attributes

con = dbConnect(drv=SQLite(), dbname="database.sqlite")
alltables = dbListTables(con)
alltables

matches_data_df <- dbGetQuery(con, "select * from  Match")
goals_data_df <- select(matches_data_df, home_team_api_id, away_team_api_id, home_team_goal, away_team_goal)
goals_data_df1 <- aggregate(home_team_goal ~ home_team_api_id, goals_data_df, sum)
#this will match home team id with away team goals

away_goals_data_df1 <- aggregate(away_team_goal ~ away_team_api_id, goals_data_df, sum)
#this will match away team id with away team goals

colnames(away_goals_data_df1)[1] <- "home_team_api_id"
#shortcut here to name away team id to home team id for easy merging.

total_goalsbyteam_df <- merge(goals_data_df1, away_goals_data_df1, by = "home_team_api_id")
total_goalsbyteam_df1 <- mutate(total_goalsbyteam_df, totalgoals = home_team_goal + away_team_goal)
total_goalsbyteam_final_df <- select(total_goalsbyteam_df1, home_team_api_id, totalgoals)
#merge the home and away goals, mutate function to create new column for total, then isolate into 2 columns

dbListFields(con, "Team")
TeamData_df <- dbGetQuery(con, "select * from  Team")
isolateTeamName <- select(TeamData_df, team_api_id, team_long_name)
#get the original categories from Team to merge

colnames(total_goalsbyteam_final_df)[1] <- "team_api_id"
#change name to allow merge

finaldf <- merge(isolateTeamName, total_goalsbyteam_final_df, by = "team_api_id")
#merge the data

teamwith_totalgoals <- select(finaldf, team_long_name, totalgoals)
teamwith_totalgoals1 <- aggregate(totalgoals ~ team_long_name, teamwith_totalgoals, max)
teamwith_totalgoals_final <- arrange(teamwith_totalgoals1, desc(totalgoals))
#select the dataframe by team_long_name and total goals and sort by max

teamsbyscoredgoals <- select(head(teamwith_totalgoals_final, 10), team_long_name)
#print out top 10 teams by scored goals


