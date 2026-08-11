.bail on
.open --new /tmp/rusqdoltlite-noncurrent-history.db
SELECT dolt_checkout('-b', 'feature');
CREATE TABLE feature_only(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO feature_only VALUES(1, 'feature');
SELECT dolt_commit('-A', '-m', 'feature-only table');
SELECT dolt_checkout('main');
.open /tmp/rusqdoltlite-noncurrent-history.db
SELECT 'history_rows=' || count(*)
FROM dolt_history_feature_only
WHERE commit_hash = dolt_hashof('feature');
