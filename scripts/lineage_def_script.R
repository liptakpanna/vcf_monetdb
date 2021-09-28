library(tidyverse)
library(DBI)
library(MonetDB.R)

print(paste(Sys.time(), "started...", sep = " "))

lineage_def <- read_delim(file = "/mnt/repo/data/table_variants_veo.csv", delim = ";")

source("scripts/util.r")

con <- dbConnect(MonetDB.R(), 
  host=getEnvVar("DB_HOST","monetdb.monetdb"), 
  dbname=getEnvVar("DB","demo"), 
  port=getEnvVar("DB_PORT","50000"),
  user=getEnvVar("SECRET_USERNAME","monetdb"), 
  password=getEnvVar("SECRET_PASSWORD","monetdb"))

dbSendUpdate(con, paste("set schema ", getEnvVar("SCHEMA","kooplex")))

lineage_def <- lineage_def %>%
  dplyr::rename(variant_id = "WHO_label")%>%
  mutate(pos=str_remove(ref_pos_alt, pattern = ref))%>%
  mutate(pos=str_remove(pos, pattern = alt)) %>%
  mutate(description="x")%>%
  dplyr::rename(ref_protein="REF_protein",
         alt_protein="ALT_protein")%>%
  filter(effect=="missense_variant",
         gene =="S",
         amino_acid_change!="D614G")
lineage_def$pos <- as.integer(lineage_def$pos)


dbSendQuery(con, "TRUNCATE TABLE lineage_def")
print(paste(Sys.time(), "truncated table lineage_def", sep=" ")) 

dbWriteTable(con, "lineage_def", lineage_def , row.names = FALSE, overwrite = TRUE)

dbDisconnect(con)


