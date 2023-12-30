// find authors who are often co-authors

MATCH (a1:Author)-[:AUTHORED]->(:Title)<-[:AUTHORED]-(a2:Author)
WITH a1, a2, count(a1) AS coauthorCount
WHERE coauthorCount > 5
RETURN a1.Name, collect(a2.Name) AS FrequentCoAuthors

// find authors who are often associated with the same category
MATCH (a1:Author)-[:AUTHORED]->(:Title)-[:IS_CATEGORIZED_AS]->(c:Category_List)<-[:IS_CATEGORIZED_AS]-(:Title)<-[:AUTHORED]-(a2:Author)
RETURN a1.Name, c.Category_List, collect(a2.Name) AS Co_Authors


// TODO install plugins
CALL algo.list()


// TODO install plugins
CALL gds.pageRank.stats('Author', {
  maxIterations: 20,
  dampingFactor: 0.85
})
YIELD centralityDistribution
RETURN centralityDistribution.max AS max