
----------------------------------------------------
-- queries to check if we have many duplicate authors
select count(*)
from author;

select count(*), full_name
from author
group by (full_name)
order by count(*) desc
limit 100;

select 1 as grp
from author
group by (full_name);

select sum(grp)
from (select 1 as grp
      from author
      group by (full_name)) as t;
----------------------------------------------------


-- see how many similarly named authors there are to Steeghs
select *
from author
where full_name LIKE 'Steeghs%';

-- rank authors by number of publications
select counts, full_name
from (select count(author_id) as counts, author_id
      from publications
      group by (author_id)
      order by count(author_id) desc
      limit 100) as t
         left join author on author.author_id = t.author_id;


-- select authors by h-index
SELECT author_id, MAX(rank) AS h_index
FROM (SELECT p.author_id,
             a.article_id,
             a.cited_by_count,
             RANK() OVER (PARTITION BY p.author_id ORDER BY a.cited_by_count DESC) AS rank
      FROM publications p
               JOIN article a ON p.article_id = a.article_id) subquery
WHERE rank <= cited_by_count
GROUP BY author_id
ORDER BY h_index DESC;


-- advanced queries!! (literally shows depth)
-- get articles by the same authors where article_id == 10. in JSON !
SELECT coalesce(json_agg("root"), '[]') AS "root"
FROM (SELECT row_to_json(
                     (SELECT "_e"
                      FROM (SELECT "_root.ar.root.publications"."publications" AS "publications",
                                   "_root.base"."title"                        AS "title",
                                   "_root.base"."abstract"                     AS "abstract") AS "_e")
             ) AS "root"
      FROM (SELECT *
            FROM "public"."article"
            WHERE (
                      ("public"."article"."article_id") = (('10') :: integer)
                      )) AS "_root.base"
               LEFT OUTER JOIN LATERAL (
          SELECT coalesce(json_agg("publications"), '[]') AS "publications"
          FROM (SELECT row_to_json(
                               (SELECT "_e"
                                FROM (SELECT "_root.ar.root.publications.or.author"."author" AS "author") AS "_e")
                       ) AS "publications"
                FROM (SELECT *
                      FROM "public"."publications"
                      WHERE (("_root.base"."article_id") = ("article_id"))) AS "_root.ar.root.publications.base"
                         LEFT OUTER JOIN LATERAL (
                    SELECT row_to_json(
                                   (SELECT "_e"
                                    FROM (SELECT "_root.ar.root.publications.or.author.ar.author.publications"."publications" AS "publications",
                                                 "_root.ar.root.publications.or.author.base"."full_name"                      AS "full_name") AS "_e")
                           ) AS "author"
                    FROM (SELECT *
                          FROM "public"."author"
                          WHERE (
                                    ("_root.ar.root.publications.base"."author_id") = ("author_id")
                                    )
                          LIMIT 1) AS "_root.ar.root.publications.or.author.base"
                             LEFT OUTER JOIN LATERAL (
                        SELECT coalesce(json_agg("publications"), '[]') AS "publications"
                        FROM (SELECT row_to_json(
                                             (SELECT "_e"
                                              FROM (SELECT "md5_c3c0706a8898cc1a5785e9177f2658da__root.ar.root.publications.or.author.ar.author.publications.or.article"."article" AS "article") AS "_e")
                                     ) AS "publications"
                              FROM (SELECT *
                                    FROM "public"."publications"
                                    WHERE (
                                                  (
                                                      "_root.ar.root.publications.or.author.base"."author_id"
                                                      ) = ("author_id")
                                              )) AS "md5_fa9de4bfd2936a5a64cb7fc66180aa42__root.ar.root.publications.or.author.ar.author.publications.base"
                                       LEFT OUTER JOIN LATERAL (
                                  SELECT row_to_json(
                                                 (SELECT "_e"
                                                  FROM (SELECT "md5_fc63b0bec1fa4ceec72a5b68f2d6faa6__root.ar.root.publications.or.author.ar.author.publications.or.article.base"."title"    AS "title",
                                                               "md5_fc63b0bec1fa4ceec72a5b68f2d6faa6__root.ar.root.publications.or.author.ar.author.publications.or.article.base"."abstract" AS "abstract") AS "_e")
                                         ) AS "article"
                                  FROM (SELECT *
                                        FROM "public"."article"
                                        WHERE (
                                                      (
                                                          "md5_fa9de4bfd2936a5a64cb7fc66180aa42__root.ar.root.publications.or.author.ar.author.publications.base"."article_id"
                                                          ) = ("article_id")
                                                  )
                                        LIMIT 1) AS "md5_fc63b0bec1fa4ceec72a5b68f2d6faa6__root.ar.root.publications.or.author.ar.author.publications.or.article.base"
                                  ) AS "md5_c3c0706a8898cc1a5785e9177f2658da__root.ar.root.publications.or.author.ar.author.publications.or.article"
                                                       ON ('true')) AS "_root.ar.root.publications.or.author.ar.author.publications"
                        ) AS "_root.ar.root.publications.or.author.ar.author.publications" ON ('true')
                    ) AS "_root.ar.root.publications.or.author" ON ('true')) AS "_root.ar.root.publications"
          ) AS "_root.ar.root.publications" ON ('true')) AS "_root";

