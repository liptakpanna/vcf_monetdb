library(tidyverse)
library(DBI)
library(MonetDB.R)

print(paste(Sys.time(), "started...", sep = " "))

lineage_def <- read_delim(file = "/mnt/repo/data/table_variants_veo.csv", delim = ";")


con<- dbConnect(MonetDB.R(), host="monetdb.monetdb", dbname="demo", user="monetdb", password="monetdb")

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


