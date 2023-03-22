from neo4j import GraphDatabase

import pandas as pd
import numpy as np
import json

from alive_progress import alive_bar

# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "bolt://localhost:17687"
AUTH = ("neo4j", "wmYnXVxdK3")

USE_MULTIINSERT = False
USE_INDEX = True
CHUNK_SIZE = 1000

OALEX_JSON =  "abstract_openalex_ids.json"
ROW_CAP = 1000

def connect_neo4j(URI, AUTH):
    with GraphDatabase.driver(URI, auth=AUTH) as driver: 
        driver.verify_connectivity() 
        return driver
    
def insert_work(tx, work):
    result = tx.run("CREATE (:Work {w_id: $id, doi: $doi, title: $title, publication_date: $publication_date, type: $type, open_access: $open_access, cited_by_count: $cited_by_count, biblio: $biblio, is_retracted: $is_retracted, is_paratext: $is_paratext, locations: $locations, ngrams_url: $ngrams_url, abstract_inverted_index: $abstract_inverted_index, cited_by_api_url: $cited_by_api_url, counts_by_year: $counts_by_year})",
                    id=work["id"],
                    doi=work["doi"],
                    title=work["title"],
                    publication_date=work["publication_date"],
                    type=work["type"],
                    open_access=json.dumps(work["open_access"]),
                    cited_by_count=int(work["cited_by_count"]),
                    biblio=json.dumps(work["biblio"]),
                    is_retracted=work["is_retracted"],
                    is_paratext=work["is_paratext"],
                    locations=json.dumps(work["locations"]),
                    ngrams_url=work["ngrams_url"],
                    abstract_inverted_index=json.dumps(work["abstract_inverted_index"]),
                    cited_by_api_url=work["cited_by_api_url"],
                    counts_by_year=json.dumps(work["counts_by_year"]))
    return result.consume()

## In development
def insert_work_list(tx, works):
    query = """
    UNWIND $works as work
    WITH work, apoc.map.fromPairs([
        ["open_access", work.open_access],
        ["biblio", work.biblio],
        ["locations", work.locations],
        ["abstract_inverted_index", work.abstract_inverted_index],
        ["counts_by_year", work.counts_by_year]
    ]) as properties
    CREATE (:Work {id: work.id, doi: work.doi, title: work.title, publication_date: work.publication_date, type: work.type, open_access: properties.open_access, cited_by_count: work.cited_by_count, biblio: properties.biblio, is_retracted: work.is_retracted, is_paratext: work.is_paratext, locations: properties.locations, ngrams_url: work.ngrams_url, abstract_inverted_index: properties.abstract_inverted_index, cited_by_api_url: work.cited_by_api_url, counts_by_year: properties.counts_by_year})
    """
    params = {
        "works": works}
    result = tx.run(query, params)
    return result.consume()
##

def insert_venue(tx, venue):
    result = tx.run("CREATE (:Venue {v_id: $id, issn: $issn_l, name: $display_name, publisher: $publisher, type: $type, url: $url, is_oa: $is_oa, version: $version, license: $license})",
                    id=venue["id"],
                    issn_l=venue["issn_l"],
                    display_name=venue["display_name"],
                    publisher=venue["publisher"],
                    type=venue["type"],
                    url=venue["url"],
                    is_oa=venue["is_oa"],
                    version=venue["version"],
                    license=venue["license"])
    return result.consume()

## In development
def insert_venue_list(tx, venues):
    query = """
    UNWIND $venues as venue
    CREATE (:Venue {id: venue.id, issn: venue.issn_l, name: venue.display_name, publisher: venue.publisher, type: venue.type, url: venue.url, is_oa: venue.is_oa, version: venue.version, license: venue.license})
    """
    params = {"venues": venues}
    result = tx.run(query, params)
    return result.consume()
##

def insert_author(tx, author):
    result = tx.run("CREATE (:Author {a_id: $id, name: $display_name, orcid: $orcid})",
                    id=author["id"],
                    display_name=author["display_name"],
                    orcid=author["orcid"])
    return result.consume()

## In development
def insert_author_list(tx, authors):
    query = """
    UNWIND $authors as author
    CREATE (:Author {id: author.id, name: author.display_name, orcid: author.orcid})
    """
    params = {"authors": authors}
    result = tx.run(query, params)
    return result.consume()
##

def insert_institution(tx, institution):
    result = tx.run("CREATE (:Institution {i_id: $id, name: $display_name, ror: $ror, country: $country_code, type: $type})",
                    id=institution["id"],
                    display_name=institution["display_name"],
                    ror=institution["ror"],
                    country_code=institution["country_code"],
                    type=institution["type"])
    return result.consume()

## In development
def insert_institution_list(tx, institutions):
    query = """
    UNWIND $institutions as institution
    CREATE (:Institution {id: institution.id, name: institution.display_name, ror: institution.ror, country: institution.country_code, type: institution.type})
    """
    params = {"institutions": institutions}
    result = tx.run(query, params)
    return result.consume()
##

def insert_concept(tx, concept):
    result = tx.run("CREATE (:Concept {c_id: $id, name: $display_name, level: $level, wikidata: $wikidata})",
                    id=concept["id"],
                    display_name=concept["display_name"],
                    level=concept["level"],
                    wikidata=concept["wikidata"])
    return result.consume()

## In development
def insert_concept_list(tx, concepts):
    query = """
    UNWIND $concepts as concept
    CREATE (:Concept {id: concept.id, name: concept.display_name, level: concept.level, wikidata: concept.wikidata})
    """
    params = {"concepts": concepts}
    result = tx.run(query, params)
    return result.consume()
##


### RELATIONSHIPS ###

def insert_authorship(tx, work, authorship):
    result = tx.run("MATCH (a:Author {a_id: $author_id}), (w:Work {w_id: $work_id}) "
                    "MERGE (a)-[:IS_AUTHOR]->(w)",
                    author_id=authorship["author"]["id"],
                    work_id=work["id"])
    return result.consume()

def insert_authorship_index(tx, work, authorship):
    result = tx.run("MATCH (a:Author) USING INDEX a:Author(a_id) WHERE a.a_id = $author_id "
                    "MATCH (w:Work) USING INDEX w:Work(w_id) WHERE w.w_id = $work_id "
                    "MERGE (a)-[:IS_AUTHOR]->(w)",
                    author_id=authorship["author"]["id"],
                    work_id=work["id"])
    return result.consume()

# def insert_authorship_institution(tx, work, authorship, institution):
#     result = tx.run("MATCH (a:Author {a_id: $author_id}), (w:Work {w_id: $work_id}), (i:Institution {i_id: $institution_id}) "
#                     "MERGE (a)-[:IS_AUTHOR]->(w)-[:IS_FROM]->(i)", author_id=authorship["author"]["id"],
#                     work_id=work["id"],
#                     institution_id=institution["id"])
#     return result.consume()

# def insert_authorship_institution_index(tx, work, authorship, institution):
#     result = tx.run("MATCH (a:Author) USING INDEX a:Author(a_id) WHERE a.a_id = $author_id "
#                     "MATCH (w:Work) USING INDEX w:Work(w_id) WHERE w.w_id = $work_id "
#                     "MATCH (i:Institution) USING INDEX i:Institution(i_id) WHERE i.i_id = $institution_id "
#                     "MERGE (a)-[:IS_AUTHOR]->(w)-[:IS_FROM]->(i)",
#                     author_id=authorship["author"]["id"],
#                     work_id=work["id"],
#                     institution_id=institution["id"])
#     return result.consume()

def insert_authorship_institution(tx, work, authorship, institution):
    result = tx.run("MATCH (a:Author {a_id: $author_id}) (i:Institution {i_id: $institution_id}) "
                    "MERGE (a)-[:IS_FROM {through_work: $work_id}]->(i)", author_id=authorship["author"]["id"],
                    work_id=work["id"],
                    institution_id=institution["id"])
    return result.consume()

# def insert_authorship_institution_index(tx, work, authorship, institution):
#     result = tx.run("MATCH (a:Author) USING INDEX a:Author(a_id) WHERE a.a_id = $author_id "
#                     "MATCH (i:Institution) USING INDEX i:Institution(i_id) WHERE i.i_id = $institution_id "
#                     "MERGE (a)-[:IS_FROM {through_work: $work_id}]->(i)",
#                     author_id=authorship["author"]["id"],
#                     work_id=work["id"],
#                     institution_id=institution["id"])
#     return result.consume()

def insert_authorship_institution_index(tx, work, authorship, institution):
    result = tx.run("MATCH (a:Author) USING INDEX a:Author(a_id) WHERE a.a_id = $author_id "
                    "MATCH (i:Institution) USING INDEX i:Institution(i_id) WHERE i.i_id = $institution_id "
                    "MERGE (a)-[r:IS_FROM]->(i) "
                    "ON MATCH SET r.through_work = COALESCE(r.through_work, []) + $work_id ",
                    author_id=authorship["author"]["id"],
                    work_id=work["id"],
                    institution_id=institution["id"])
    return result.consume()

def insert_host_venue(tx, work):
    result = tx.run("MATCH (v:Venue {v_id: $venue_id}), (w:Work {w_id: $work_id}) "
                    "MERGE (w)-[:IS_PUBLISHED_IN]->(v)",
                    venue_id=work["host_venue"]["id"],
                    work_id=work["id"])
    return result.consume()

def insert_host_venue_index(tx, work):
    result = tx.run("MATCH (v:Venue) USING INDEX v:Venue(v_id) WHERE v.v_id = $venue_id "
                    "MATCH (w:Work) USING INDEX w:Work(w_id) WHERE w.w_id = $work_id "
                    "MERGE (w)-[:IS_PUBLISHED_IN]->(v)",
                    venue_id=work["host_venue"]["id"],
                    work_id=work["id"])
    return result.consume()

def insert_concept_work(tx, work, concept):
    for individual_concept in work["concepts"]:
        # Assign confidence to concept - work relationships
        result = tx.run("MATCH (c:Concept {c_id: $concept_id}), (w:Work {w_id: $work_id}) "
                        "MERGE (w)-[:IS_ABOUT {confidence: $confidence}]->(c)",
                        concept_id=individual_concept["id"],
                        work_id=work["id"],
                        confidence=individual_concept["score"])
    return result.consume()

def insert_concept_work_index(tx, work, concept):
    for individual_concept in work["concepts"]:
        # Assign confidence to concept - work relationships
        result = tx.run("MATCH (c:Concept) USING INDEX c:Concept(c_id) WHERE c.c_id = $concept_id "
                        "MATCH (w:Work) USING INDEX w:Work(w_id) WHERE w.w_id = $work_id "
                        "MERGE (w)-[:IS_ABOUT {confidence: $confidence}]->(c)",
                        concept_id=individual_concept["id"],
                        work_id=work["id"],
                        confidence=float(individual_concept["score"]))
    return result.consume()


def remove_nested_items(d):
    """
    Recursively removes nested items from a dictionary.
    """
    result = {}
    for k, v in d.items():
        if isinstance(v, dict):
            result.update(remove_nested_items(v))
        else:
            result[k] = v
    return result

with alive_bar() as bar:
    oalex = pd.read_json(OALEX_JSON, lines=True, orient="records", dtype={"id": "string"})
    print("Number of rows in JSON: " + str(len(oalex)))
    
    if ROW_CAP:
        oalex = oalex[:ROW_CAP]
        print("Number of rows after capping: " + str(len(oalex)))

works = []
institutions = []
authors = []
concepts = []
venues = []

# Extract all works, institutions, authors, concepts and venues as separate lists and remove duplicates
with alive_bar(len(oalex)) as bar:
    for index, row in oalex.iterrows():
        if row['oalex_data'] is not None:
            for oalex_works in row['oalex_data']:
                works.append(oalex_works)
                if oalex_works['authorships'] is not None:
                    for authorship in oalex_works['authorships']:
                        authors.append(authorship["author"])  
                        if authorship["institutions"] is not None:
                            for institution in authorship["institutions"]:
                                institutions.append(institution)
                if oalex_works['concepts'] is not None:
                    for concept in oalex_works['concepts']:
                        concepts.append({"id":concept["id"], "wikidata": concept["wikidata"], "display_name": concept["display_name"], "level": concept["level"]})
                if oalex_works['host_venue'] is not None:
                    venues.append(oalex_works['host_venue'])
        bar()

# Drop institutions, authors, concepts and venues that have no id
institutions = [d for d in institutions if d.get("id")]
authors = [d for d in authors if d.get("id")]
concepts = [d for d in concepts if d.get("id")]
venues = [d for d in venues if d.get("id")]

# Drop exact duplicates in these lists, they contain dictionaries
unique_institutions = set()
institutions_set = [d for d in institutions if d.get("id") is not None and d.get("id") not in unique_institutions and (unique_institutions.add(d.get("id")) or True)]
unique_authors = set()
authors_set = [d for d in authors if d.get("id") is not None and d.get("id") not in unique_authors and (unique_authors.add(d.get("id")) or True)]
unique_concepts = set()
concepts_set = [d for d in concepts if d.get("id") is not None and d.get("id") not in unique_concepts and (unique_concepts.add(d.get("id")) or True)]
unique_venues = set()
venues_set = [d for d in venues if d.get("id") is not None and d.get("id") not in unique_venues and (unique_venues.add(d.get("id")) or True)]


print("Number of works: " + str(len(works)))
print("Number of institutions: " + str(len(institutions_set)) + " (- " + str(len(institutions) - len(institutions_set)) + ")")
print("Number of authors: " + str(len(authors_set)) + " (- " + str(len(authors) - len(authors_set)) + ")")
print("Number of concepts: " + str(len(concepts_set)) + " (- " + str(len(concepts) - len(concepts_set)) + ")")
print("Number of venues: " + str(len(venues_set)) + " (- " + str(len(venues) - len(venues_set)) + ")")

driver = connect_neo4j(URI, AUTH)
with driver.session(database="neo4j") as session:
    # Insert works, institutions, authors, concepts and venues as nodes in Neo4j
    
    # Check, if nodes already exist
    result = session.run("MATCH (n) RETURN n.id")
    existing_ids = [record["n.id"] for record in result]
    
    if len(existing_ids) == len(works) + len(institutions_set) + len(authors_set) + len(concepts_set) + len(venues_set):
        print("Nodes already exist in Neo4j.")
        print("Number of existing nodes: " + str(len(existing_ids)))
    else:
        print("Existing nodes are not equal to new nodes. Creating them...")
        
        # Delete all nodes
        session.run("MATCH (n) DETACH DELETE n")
        
        if USE_MULTIINSERT:
            # Convert nested items to strings
            # works = [remove_nested_items(work) for work in works]
            # works = [dict((k, json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in work.items()) for work in works]
            # works = [dict((k, [json.dumps(d) if isinstance(d, dict) else d for d in v] if k == "counts_by_year" else v) for k, v in work.items()) for work in works]
            print(works[0])
            summary = insert_work_list(session, works)
            print("Inserted works: " + str(summary.counters.nodes_created))
            summary = insert_institution_list(session, institutions_set)
            print("Inserted institutions: " + str(summary.counters.nodes_created))
            summary = insert_author_list(session, authors_set)
            print("Inserted authors: " + str(summary.counters.nodes_created))
            summary = insert_concept_list(session, concepts_set)
            print("Inserted concepts: " + str(summary.counters.nodes_created))
            summary = insert_venue_list(session, venues_set)
            print("Inserted venues: " + str(summary.counters.nodes_created))
            
        else:
            
            # Drop all existing indexes
            result = session.run("SHOW INDEXES")
            indexes = [record["name"] for record in result if record["name"] is not None]
            # print(indexes)
            for index in indexes:
                session.run("DROP INDEX " + index)
    
            with alive_bar(len(works), title="Inserting Works") as bar:
                for work in works:
                    session.write_transaction(insert_work, work)
                    bar()
            with alive_bar(len(institutions_set), title="Inserting Institutions") as bar:
                for institution in institutions_set:
                    session.write_transaction(insert_institution, institution)
                    bar()
            with alive_bar(len(authors_set), title="Inserting Authors") as bar:
                for author in authors_set:
                    session.write_transaction(insert_author, author)
                    bar()
            with alive_bar(len(concepts_set), title="Inserting Concepts") as bar:
                for concept in concepts_set:
                    session.write_transaction(insert_concept, concept)
                    bar()
            with alive_bar(len(venues_set), title="Inserting Venues") as bar:
                for venue in venues_set:
                    session.write_transaction(insert_venue, venue)
                    bar()
                    
    if USE_INDEX:
        
        print("Creating indexes on nodes...")
        
        with alive_bar(title="Creating indexes on works"):
            # Check if indexes already exist
            result = session.run("SHOW INDEXES")
            indexes = [item for sublist in [record["properties"] for record in result if record["properties"] is not None] for item in sublist if item is not None]

            if "w_id" in indexes:
                print("Indexes already exist on works.")
            else:
                # Create indexes for nodes
                session.run("CREATE INDEX FOR (n:Work) ON (n.w_id)")
                print("Created indexes on works.")
            if "a_id" in indexes:
                print("Indexes already exist on authors.")
            else:
                # Create indexes for nodes
                session.run("CREATE INDEX FOR (n:Author) ON (n.a_id)")
                print("Created indexes on authors.")
            if "i_id" in indexes:
                print("Indexes already exist on institutions.")
            else:
                # Create indexes for nodes
                session.run("CREATE INDEX FOR (n:Institution) ON (n.i_id)")
                print("Created indexes on institutions.")
            if "c_id" in indexes:
                print("Indexes already exist on concepts.")
            else:
                # Create indexes for nodes
                session.run("CREATE INDEX FOR (n:Concept) ON (n.c_id)")
                print("Created indexes on concepts.")
            if "v_id" in indexes:
                print("Indexes already exist on venues.")
            else:
                # Create indexes for nodes
                session.run("CREATE INDEX FOR (n:Venue) ON (n.v_id)")
                print("Created indexes on venues.")
            
        # Insert relationships between works, institutions, authors, concepts and venues based on the works
        with alive_bar(len(works)) as bar:
            for work in works:
                if work['authorships'] is not None:
                    for authorship in work['authorships']:
                        session.write_transaction(insert_authorship, work, authorship)
                        if authorship['institutions'] is not None:
                            for institution in authorship['institutions']:
                                session.write_transaction(insert_authorship_institution_index, work, authorship, institution)
                if work['concepts'] is not None:
                    for concept in work['concepts']:
                        session.write_transaction(insert_concept_work_index, work, concept)
                if work['host_venue'] is not None:
                    session.write_transaction(insert_host_venue_index, work)
                bar()

            
        
    else:
        print("Not creating indexes on nodes.")
                
        # Insert relationships between works, institutions, authors, concepts and venues based on the works
        with alive_bar(len(works)) as bar:
            for work in works:
                if work['authorships'] is not None:
                    for authorship in work['authorships']:
                        session.write_transaction(insert_authorship, work, authorship)
                        if authorship['institutions'] is not None:
                            for institution in authorship['institutions']:
                                session.write_transaction(insert_authorship_institution, work, authorship, institution)
                if work['concepts'] is not None:
                    for concept in work['concepts']:
                        session.write_transaction(insert_concept_work, work, concept)
                if work['host_venue'] is not None:
                    session.write_transaction(insert_host_venue, work)
                bar()

