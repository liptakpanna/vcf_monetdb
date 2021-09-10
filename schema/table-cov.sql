CREATE TABLE IF NOT EXISTS cov(
    ena_run     varchar(20),       -- ENA ID
    pos         int,               -- Position in the sequence
    coverage    int,                -- Coverage in the given position
    CONSTRAINT pk_cov PRIMARY KEY (ena_run,pos)
);
