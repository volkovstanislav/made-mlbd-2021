-- Копируем датасет локальную систему контейнера docker cp artists.csv 51d3a656f8a8:/artists.csv

-- Создаем таблицу
DROP TABLE IF EXISTS artists;

CREATE TABLE artists (
	mbid STRING, 
	artist_mb STRING, 
	artist_lastfm STRING, 
	country_mb STRING, 
	country_lastfm STRING, 
	tags_mb STRING, 
	tags_lastfm STRING, 
	listeners_lastfm INT, 
	scrobbles_lastfm INT, 
	ambiguous_artist BOOLEAN)

 ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
 tblproperties ("skip.header.line.count"="1");

-- Заливаем csv в только что созданную таблицу
LOAD DATA LOCAL INPATH '/opt/artists.csv' OVERWRITE INTO TABLE artists;

-- Формируем запросы
 -- 1 Исполнителя с максимальным числом скробблов - 5 баллов
SELECT artist_lastfm
FROM
 (SELECT artist_lastfm, scrobbles_lastfm, 
         RANK() OVER (ORDER BY CAST(scrobbles_lastfm AS FLOAT) DESC) as raiting 
  FROM artists) T 
WHERE R.raiting = 1;


-- 2 Самый популярный тэг на ластфм - 10 баллов
 -- шаг 1 разбиваем массивы на слова
 -- шаг 2 группируем, cчитаем кол-во
 -- шаг 3 сортируем и оставляем топ-1
SELECT tags, COUNT(*) AS cnt
FROM artists LATERAL VIEW explode(split(tags_lastfm, ';')) l as tags
WHERE tags <> ''
GROUP BY tags
SORT BY cnt DESC
LIMIT 1;


-- 3 Самые популярные исполнители 10 самых популярных тегов ластфм - 10 баллов
-- создаем таблицу из топ-10 самых популярных тега
CREATE TABLE top10 as
SELECT T.tags
FROM (
    SELECT tags, COUNT(*) AS cnt
    FROM artists LATERAL VIEW explode(split(tags_lastfm, ';')) l as tags
    WHERE tags <> ''
    GROUP BY tags
    SORT BY cnt DESC
    LIMIT 10
) T

-- создаем таблицу из исполнителей и кол-во прослушиваний в топ10 топ-10 самых популярных тега
CREATE TABLE top_artists as
SELECT artists.artist_lastfm, artists.listeners_lastfm
FROM artists LATERAL VIEW explode(split(tags_lastfm, ';')) artists as tagslm
WHERE artists.tagslm IN (SELECT tags FROM top10)
ORDER BY artists.listeners_lastfm DESC;

-- отбираем топ10 самых популрных по кол-ву прослушиваний
SELECT T.artist_lastfm
FROM (
    SELECT DISTINCT artist_lastfm, listeners_lastfm
    FROM top_artists
    ORDER BY listeners_lastfm DESC
    LIMIT 10
) T


-- 4 любой другой инсайт на ваше усмотрение - 10 баллов
-- Топ-10 популярных исполнителей (по скробблам) в России в категории рок. Скромно, но со вкусом :)
SELECT artist_lastfm, SUM(scrobbles_lastfm) as sum_list 
FROM artists LATERAL VIEW explode(split(tags_lastfm, ';')) l as tags
WHERE tags = 'rock' AND country_lastfm = 'Russia'
GROUP BY artist_lastfm
SORT BY sum_list DESC
LIMIT 10;


