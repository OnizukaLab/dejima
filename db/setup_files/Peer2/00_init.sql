CREATE TABLE BT (
	ID	serial primary key,
	COL1	varchar,
	COL2	varchar,
	COL3	varchar,
	COL4	varchar,
	COL5	varchar,
	COL6	varchar,
	COL7	varchar,
	COL8	varchar,
	COL9	varchar,
	COL10	varchar,
	LINEAGE	varchar,
	COND1	int,
	COND2	int,
	COND3	int,
	COND4	int,
	COND5	int,
	COND6	int,
	COND7	int,
	COND8	int,
	COND9	int,
	COND10	int
);

CREATE INDEX ON bt (lineage);
CREATE INDEX ON bt (COND1);
CREATE INDEX ON bt (COND2);
CREATE INDEX ON bt (COND3);
CREATE INDEX ON bt (COND4);
CREATE INDEX ON bt (COND5);
CREATE INDEX ON bt (COND6);
CREATE INDEX ON bt (COND7);
CREATE INDEX ON bt (COND8);
CREATE INDEX ON bt (COND9);
CREATE INDEX ON bt (COND10);
