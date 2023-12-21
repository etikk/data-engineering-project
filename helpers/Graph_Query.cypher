//Main script
CALL apoc.load.json("file:/partition_test.json") YIELD value AS author
MERGE (b:Title {Title: author.title})  // Title
//SET b.Title = author.title
WITH b,author
UNWIND author.authors_parsed AS author_parsed
MERGE (a:Author {firstName: author_parsed[1], lastName: author_parsed[0]})
MERGE (a)-[:Authored]->(b)
WITH b,author
UNWIND author.submitter as sumbitter_parsed
MERGE (c:Author {firstName: sumbitter_parsed[1], lastName: sumbitter_parsed[0]})  // Submitter
//SET c.submitter = author.submitter
MERGE (c)-[:SUBMITTED]->(b)
WITH b,author
WHERE author.categories IS NOT NULL
MERGE (d:Categories {Categories : author.categories}) // Categories
//SET d.Categories = author.categories
MERGE (b)-[:IS_CATEGORIZED_AS]->(d)
WITH b,author
WHERE author.journal_ref IS NOT NULL
MERGE (e:Journal_Reference {Journal_Reference : author.journal_ref}) // Journal_Ref
//SET d.Categories = author.categories
MERGE (b)-[:Referenced_In_Journal]->(e)




// Find the person who has authored the most papers and give the most common category


MATCH (a:Author)-[:Authored]->(t:Title)
WITH a, count(t) AS Papers
ORDER BY Papers DESC
LIMIT 1

CALL {
    WITH a
    MATCH (a:Author)-[:Authored]->(t:Title)-[:IS_CATEGORIZED_AS]->(c:Categories)
    RETURN c.Categories AS Cat, count(*) AS Cnt
    ORDER BY Cnt DESC
    LIMIT 1
}

RETURN a.firstName, a.lastName, Papers,Cat

