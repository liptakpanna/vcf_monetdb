CREATE VIEW IF NOT EXISTS lineage%%POSTFIX%% AS
SELECT * FROM lineage_base%%POSTFIX%%
UNION
SELECT * FROM lineage_other%%POSTFIX%%
UNION
SELECT * FROM lineage_not_analyzed%%POSTFIX%%;
