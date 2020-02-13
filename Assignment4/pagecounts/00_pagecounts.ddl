-- define an external table over raw pagecounts data
CREATE TABLE IF NOT EXISTS pagecounts (projectcode STRING, pagename STRING, pageviews STRING, bytes STRING)
ROW FORMAT
  DELIMITED FIELDS TERMINATED BY ' '
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hduser/pagecounts';

