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
 -- шаг 2 группируем, cсчитаем кол-во
 -- шаг 3 сортируем и оставляем топ-1
SELECT TAB.tags, COUNT(*) as count
FROM (
    SELECT EXPLODE(SPLIT(tags_lastfm, ';')) as tags
    FROM artists) TAB
GROUP BY TAB.tags
SORT BY count DESC
LIMIT 1;

-- 3 Самые популярные исполнители 10 самых популярных тегов ластфм - 10 баллов

 -- выводим топ популярных артистов
SELECT RS.artist_lastfm
FROM (
	-- при помощи ранка отранжируем в порядке убывания по кол-во прослушиваний и оставим топ-10 :)
    SELECT TG.artist_lastfm, RANK() OVER (ORDER BY TG.cnt DESC) AS raiting
	FROM (
		-- формируем таблицу формата артист и кол-во прослушиваний для топ-10 самых популярных тэгов
    	SELECT artist_lastfm, SUM(listeners_lastfm) as cnt 
		FROM artists LATERAL VIEW explode(split(tags_lastfm, ';')) l as tags
		WHERE tags IN ( 
			-- Отбираем топ-10 самых популярных тегов
		    SELECT T.tags
		    FROM (
		        SELECT TAB.tags, COUNT(*) as cnt
		        FROM (
		            SELECT EXPLODE(SPLIT(tags_lastfm, ';')) as tags
		            FROM artists) TAB
		            GROUP BY TAB.tags
		            SORT BY cnt DESC
		            LIMIT 10
		            ) T
		        )
		GROUP BY tags, artist_lastfm
    ) TG
) RS 
WHERE RS.raiting <= 10

-- 4 любой другой инсайт на ваше усмотрение - 10 баллов
 -- Топ-10 популярных исполнителей (по прослушиваниям) в России в категории рок :)
SELECT artist_lastfm, SUM(listeners_lastfm) as sum_list 
FROM artists LATERAL VIEW explode(split(tags_lastfm, ';')) l as tags
WHERE tags = 'rock' AND country_lastfm = 'Russia'
GROUP BY artist_lastfm
SORT BY sum_list DESC
LIMIT 10;


