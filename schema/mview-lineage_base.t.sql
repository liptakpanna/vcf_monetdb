CREATE OR REPLACE VIEW lineage_base%%POSTFIX%% AS
SELECT DISTINCT(ena_run) ena_run, variant_id, n, required_mutation FROM lineage0%%POSTFIX%% ORDER BY ena_run, required_mutation DESC;

