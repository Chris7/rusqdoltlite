.bail on
.open --new /tmp/rusqdoltlite-noncurrent-diff.db
SELECT dolt_checkout('-b', 'feature');
CREATE TABLE feature_only(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO feature_only VALUES(1, 'feature');
SELECT dolt_commit('-A', '-m', 'feature-only table');
SELECT dolt_checkout('main');
.open /tmp/rusqdoltlite-noncurrent-diff.db
SELECT 'diff_rows=' || count(*)
FROM dolt_diff_feature_only('main', 'feature');
