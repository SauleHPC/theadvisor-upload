from pymongo import MongoClient
import uuid
import sys

# Setup MongoDB client connections
client = MongoClient('localhost', 11111)
mag_db = client['theadvisor']
dblp_db = client['theadvisor']
matched_db = client['theadvisor']
theadvisor_db = client['theadvisor']

# Access collections
mag_collection = mag_db['mag']
dblp_collection = dblp_db['dblp']
matched_collection = matched_db['match']
theadvisor_collection = theadvisor_db['theadvisor_papers']

# Ensure indexes are created on the 'paper_id' field for mag and dblp collections
mag_collection.create_index([("paper_id", 1)])
dblp_collection.create_index([("paper_id", 1)])

# Drop theadvisor collection to start fresh
theadvisor_collection.drop()
print("Cleared the 'theadvisor_papers' collection.")

# Function to insert batches of advisor papers
def insert_batch(batch):
    if batch:
        theadvisor_collection.insert_many(batch)
        print(f"Inserted batch of {len(batch)} TheAdvisor papers.")

# Batch list
advisor_papers_batch = []

try:
    for matched_record in matched_collection.find():
        mag_record = mag_collection.find_one({"paper_id": matched_record["mag_id"]})
        dblp_record = dblp_collection.find_one({"paper_id": matched_record["best_candidate_paper_dblp_id"]})
        
        if mag_record and dblp_record:
            advisor_paper = {
                "_id": str(uuid.uuid4()),
                "mag_id": mag_record["paper_id"],
                "dblp_id": dblp_record["paper_id"],
                "title": mag_record.get("title", dblp_record.get("title", "No title available")),
                "author": dblp_record.get("author", "Unknown author"),
                "year": mag_record.get("year", dblp_record.get("year", "Unknown year")),
                "doi": dblp_record.get("doi", "No DOI available"),
                "url": dblp_record.get("url", "No URL available"),
                "matched_data_id": matched_record["_id"],
                "citation_count": mag_record.get("citation_count", 0)
            }
            advisor_papers_batch.append(advisor_paper)

            # Insert in batches of 1000
            if len(advisor_papers_batch) >= 1000:
                insert_batch(advisor_papers_batch)
                advisor_papers_batch = []

    # Insert any remaining papers
    insert_batch(advisor_papers_batch)
    print("Data loading completed.")

except Exception as e:
    print("An error occurred:", e)
    sys.exit(1)

# Check if there are entries in the MAG collection
print("MAG collection count:", mag_collection.count_documents({}))

# Check if there are entries in the DBLP collection
print("DBLP collection count:", dblp_collection.count_documents({}))

# Check if there are entries in the matched records collection
print("Matched collection count:", matched_collection.count_documents({}))
