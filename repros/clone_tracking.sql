.bail on
.open --new /tmp/rusqdoltlite-clone-tracking-remote.db
CREATE TABLE widgets(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO widgets VALUES(1, 'remote');
SELECT dolt_commit('-A', '-m', 'remote data');
.open :memory:
SELECT dolt_clone('file:///tmp/rusqdoltlite-clone-tracking-remote.db');
SELECT 'tracking_matches=' ||
       (dolt_hashof('origin/main') = dolt_hashof('main'));
