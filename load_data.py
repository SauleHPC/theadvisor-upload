import sys
from pymongo import MongoClient
from Parse import parse_DBLP_file, Paper

# Setup MongoDB connection
client = MongoClient('localhost', 11111)
db = client['theadvisor']
collection = db['dblp']

db.dblp.create_index([("paper_id", 1)])

# Global variable to accumulate papers for batch insertion
paper_batch = []

def insert_into_mongodb(paper):
    global paper_batch  # Reference the global list to accumulate papers
    
    # Convert the Paper object into a dictionary for MongoDB insertion
    paper_dict = paper.__dict__
    paper_batch.append(paper_dict)
    
    # When the batch reaches 100 papers, insert them into MongoDB and clear the batch
    if len(paper_batch) == 1000:
        try:
            collection.insert_many(paper_batch)
            print(f"Inserted batch of {len(paper_batch)} papers.")
        except Exception as e:
            print(f"Error inserting batch into MongoDB: {e}")
        paper_batch.clear()  # Clear the batch after insertion

def load_papers_in_batches(file_path):
    # Clear the collection before loading new data
    collection.drop()
    print("Collection cleared.")

    # Callback function to insert papers into MongoDB
    callback = [insert_into_mongodb]

    # Load papers from DBLP dataset in batches
    print("Starting to load papers from DBLP...")
    parse_DBLP_file(callback, 0, sys.maxsize)  # Use sys.maxsize to attempt to parse the entire file
    print("Finished loading papers from DBLP.")
    
    # Insert any remaining papers in the batch
    if paper_batch:  # Check if there are any papers left in the batch
        try:
            collection.insert_many(paper_batch)
            print(f"Inserted the final batch of {len(paper_batch)} papers.")
        except Exception as e:
            print(f"Error inserting the final batch into MongoDB: {e}")


if __name__ == "__main__":
    load_papers_in_batches('dblp-2023-05-11.xml.gz')
