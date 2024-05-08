import csv
import os
from pymongo import MongoClient

# MongoDB connection setup
client = MongoClient('localhost', 11111)
db = client['theadvisor']
collection = db['match']

def insert_papers_batch(papers_batch):
    try:
        if papers_batch:  # Ensure the batch is not empty
            result = collection.insert_many(papers_batch)
            print(f"Inserted batch with {len(result.inserted_ids)} papers")
    except Exception as e:
        print(f"An error occurred while inserting the batch: {e}")

def load_mag_papers_in_batches(file_path, batch_size=1000):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        papers_batch = []
        for row in reader:
            # Convert row to dictionary and add to the current batch
            papers_batch.append({key: value for key, value in row.items()})
            # When the batch size is reached, insert the batch into MongoDB
            if len(papers_batch) == batch_size:
                insert_papers_batch(papers_batch)
                papers_batch = []  # Reset the batch
        # Insert any remaining papers as the final batch
        insert_papers_batch(papers_batch)

if __name__ == "__main__":
    cwd = os.getcwd()
    # Assuming 'mag_to_dblp_query_total.csv' is in the current working directory
    file_path = os.path.join(cwd, 'mag_to_dblp_query_total.csv')
    collection.drop()  # Clear the collection before loading new data
    print("Collection cleared.")

    load_mag_papers_in_batches(file_path)
    print("Finished loading matched papers.")