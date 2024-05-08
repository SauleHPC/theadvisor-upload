import sys
from pymongo import MongoClient
from Parse import parse_MAG_file, Paper

# Setup MongoDB connection
client = MongoClient('localhost', 11111)
db = client['theadvisor']
collection = db['mag']
db.mag.create_index([("paper_id", 1)])

# Define a global list to hold a batch of papers
batch_of_papers = []

def insert_paper_batch_to_mongodb():
    """Inserts the batch of papers into MongoDB and clears the batch."""
    global batch_of_papers
    if batch_of_papers:
        collection.insert_many(batch_of_papers)
        print(f"Inserted batch of {len(batch_of_papers)} papers.")
        batch_of_papers.clear()

def accumulate_paper(paper):
    """Accumulates papers into a batch and inserts the batch when it reaches 1000 papers."""
    global batch_of_papers
    paper_dict = paper.__dict__
    batch_of_papers.append(paper_dict)

    # Check if the batch size reached 1000
    if len(batch_of_papers) == 1000:
        insert_paper_batch_to_mongodb()

def load_mag_papers(file_path, start_line=0, count_to=sys.maxsize):
    """Loads MAG papers from the specified file into MongoDB in batches."""
    # Clear the collection before loading new data
    collection.drop()
    print("MAG papers collection cleared.")

    # Define the callback function list
    callbacks = [accumulate_paper]

    # Start loading MAG papers
    print("Starting to load MAG papers...")
    processed_papers = parse_MAG_file(callbacks, start_line, count_to)

    # Insert any remaining papers in the batch
    insert_paper_batch_to_mongodb()

    print(f"Finished loading MAG papers. Total processed papers: {processed_papers}")

if __name__ == "__main__":
    file_path = 'Papers.txt.gz'
    load_mag_papers(file_path)
