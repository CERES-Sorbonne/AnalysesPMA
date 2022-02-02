-- populate les real_texts: 2 360 000 rows

update import.pma_full as toto set real_text = (select coalesce(source->>'full_text', source->>'text') from
public.pma_tweets as tata where
toto.id = tata.id);

-- créer la table sans duplicate: 2 220 000 rows

create table pma_uniques as select distinct * from import.pma_full;

-- recupérer le nombre de tweets collectés à la deuxieme collecte -> 1 700 000

select count(*)
from pma_uniques
where 
	real_text is not null ;

-- recuperer le nombre de tweets non pertinents collectés à la deuxieme collecte -> 112 000

select count(*)
from pma_uniques
where 
	real_text is not null 
	and substring(real_text, 0, 10) <> substring(text, 0, 10);

-- récupérer le nombre de users totaux collectés à la première collecte -> 481 067

select count(distinct from_user_name)
from pma_uniques;

-- récupérer le nombre de users totaux restant à la deuxième collecte -> 233 335
-- on a donc perdu 550 000 tweets et 250 000 users d'une collecte à l'autre

select count(distinct from_user_name)
from pma_uniques
where real_text is not null;

-- récupérer le nombre de users sur les tweets pertinents de la deuxieme collecte: 61 627
-- c'est là qu'il y a un souci, ça veut dire qu'il y aurait 233335-61627=180 000 users non pertinents alors qu'il est censé y avoir seulement 112 000 tweets non pertinents
-- donc même si chaque user n'avait tweeté que une fois ça devrait faire bcp fois plus de tweets

select count(distinct from_user_name)
from pma_uniques
where 
	real_text is not null 
	and substring(real_text, 0, 10) <> substring(text, 0, 10);

-- récupérer le nombre d'images uniques -> 77 831

select count(distinct sha1)
from pma_media;

-- récupérer le nombre d'images uniques non pertinentes -> 38 986

select count(distinct media.sha1)
from pma_uniques as uniques join pma_media as media on
uniques.id = media.tweet_id
where 
	uniques.real_text is not null 
	and substring(uniques.real_text, 0, 10) <> substring(uniques.text, 0, 10)
	and media.sha1 is not null;


-- récupérer les tweets entre deux dates de manif

select count(*) from pma_uniques where created_at > '2019-10-06' and created_at < '2019-10-07'; -- 68359
select count(*) from pma_uniques where created_at > '2020-10-10' and created_at < '2020-10-11'; -- 23850
select count(*) from pma_uniques where created_at > '2021-01-31' and created_at < '2021-02-01'; -- 9624
select count(*) from pma_uniques where created_at > '2021-06-08' and created_at < '2021-06-09'; -- 7069


-- récupérer tous les sha1 apparaissant au moins trois fois dans le corpus

with req as (select media.sha1, count(*) nb from
pma_media as media 
group by media.sha1
having count(*) > 2)
select media.sha1, tweets.created_at, tweets.from_user_name from 
pma_uniques as tweets join 
pma_media as media on
tweets.id = media.tweet_id
join req 
on media.sha1 = req.sha1
order by sha1 desc;


	
-- récupérer tous les sha1 publiés plus de 1 fois par le même utilisateur

select from_user_name,sha1,created_at, id, nb  from (
select tweets.from_user_name, media.sha1, tweets.created_at, tweets.id,
count(*) over (partition by media.sha1, tweets.from_user_name) as nb
from pma_uniques as tweets
join pma_media as media
on media.tweet_id = tweets.id
order by tweets.from_user_name, media.sha1, tweets.created_at
) as req
where nb > 1

-- récupérer uniquement les images dont le texte parle de pma ou de gpa et dont la 2nd collecte s'est effectuée proprement

select distinct media.sha1 from pma_uniques as tweets
join pma_media as media
on media.tweet_id = tweets.id
where 
tweets.real_text is not null 
and substring(tweets.real_text, 0, 10) <> substring(tweets.text, 0, 10)
and media.sha1 is not null
and (
	tweets.text like '%PMA%'
or tweets.text like '%pma%'
or tweets.text like '%Pma%'
or tweets.text like '%GPA%'
or tweets.text like '%Gpa%'
or tweets.text like '%gpa%'
or tweets.text like '%procréation%'
or tweets.text like '%procreation%'
or tweets.text like '%Surrogate%'
or tweets.text like '%surrogate%'
)