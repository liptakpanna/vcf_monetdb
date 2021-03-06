library(tidyverse)
library(DBI)
library(MonetDB.R)

print(paste(Sys.time(), "started...", sep = " "))

data <- read_csv("https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/csv")
load("/mnt/repo/data/country_iso.Rdata") #TODO: use env var to point to extra data folder

data <- data %>%
     select(country_code, population, year_week, indicator, weekly_count) %>%
     drop_na(country_code)%>%
     pivot_wider(values_from = weekly_count, names_from = indicator)%>%
     separate(year_week, into = c("date_year", "date_week"), sep="-")%>%
     rename(iso_a3 = country_code,
            ecdc_covid_country_weekly_cases = cases,
            ecdc_covid_country_weekly_deaths = deaths)

data <- data%>%
     mutate(date_year = as.numeric(date_year),
            date_week = as.numeric(date_week),
            ecdc_covid_country_weekly_cases = as.numeric(ecdc_covid_country_weekly_cases),
            ecdc_covid_country_weekly_deaths = as.numeric(ecdc_covid_country_weekly_deaths))

fin <- country_iso %>%
     left_join(data)

print(paste(Sys.time(), "parsed", sep=" ")) 

source("scripts/util.r")

con <- dbConnect(MonetDB.R(), 
  host=getEnvVar("DB_HOST","monetdb.monetdb"), 
  dbname=getEnvVar("DB","demo"), 
  port=getEnvVar("DB_PORT","50000"),
  user=getEnvVar("SECRET_USERNAME","monetdb"), 
  password=getEnvVar("SECRET_PASSWORD","monetdb"))

dbSendUpdate(con, paste("set schema ", getEnvVar("SCHEMA","kooplex")))

dbSendQuery(con, "TRUNCATE TABLE ecdc_covid_country_weekly")
print(paste(Sys.time(), "truncated table ecdc_covid_country_weekly", sep=" ")) 

dbWriteTable(con, "ecdc_covid_country_weekly", fin , row.names = FALSE, overwrite = TRUE)

dbDisconnect(con)


