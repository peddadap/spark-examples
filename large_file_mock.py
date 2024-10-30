import numpy as np
import pandas as pd
import threading
import os
import multiprocessing

# Parameters
rows_per_chunk = 1_250_000  # Generate 1 million rows per chunk
num_chunks = 400  # Total chunks to reach ~32 GB
current_dir = os.getcwd()  # Get the current working directory

# Set directories relative to the current working directory
temp_dir = os.path.join(current_dir, 'temp_batches')  # Temporary files directory
output_dir = os.path.join(current_dir, 'data')  # Final output directory
os.makedirs(temp_dir, exist_ok=True)  # Create temp directory
os.makedirs(output_dir, exist_ok=True)  # Create output directoryreate temp directory

# Final file path in the "data" directory
file_name = os.path.join(output_dir, 'large_financial_data_32gb.csv')

# Calculate 50% of available cores
num_cores = multiprocessing.cpu_count()
num_threads = max(1, num_cores // 2)  # Use at least 1 thread

# Function to generate financial data for a chunk
def generate_financial_data(num_rows, start_id):
    data = {
        'Transaction_ID': np.arange(start_id, start_id + num_rows),
        'Timestamp': pd.date_range('2023-01-01', periods=num_rows, freq='S').astype(str),
        'Account_Number': np.random.randint(1000000000, 9999999999, num_rows),
        'Transaction_Type': np.random.choice(['Credit', 'Debit'], num_rows),
        'Transaction_Amount': np.round(np.random.uniform(1.0, 10000.0, num_rows), 2),
        'Currency': np.random.choice(['USD', 'EUR', 'GBP', 'JPY'], num_rows),
        'Location': np.random.choice(['New York', 'London', 'Tokyo', 'Berlin'], num_rows),
        'Merchant': np.random.choice(['Amazon', 'Walmart', 'Target', 'BestBuy'], num_rows),
        'Card_Type': np.random.choice(['Visa', 'MasterCard', 'Amex', 'Discover'], num_rows),
        'Approval_Status': np.random.choice(['Approved', 'Declined', 'Pending'], num_rows)
    }
    return pd.DataFrame(data)

# Function to generate data and write to a temporary file
def generate_and_write_chunk(chunk_num):
    start_id = chunk_num * rows_per_chunk
    df = generate_financial_data(rows_per_chunk, start_id)
    temp_file = os.path.join(temp_dir, f'batch_{chunk_num}.csv')
    df.to_csv(temp_file, index=False)
    print(f"Chunk {chunk_num + 1}/{num_chunks} written to {temp_file}.")

# Main function to manage threads and temp files
def generate_large_csv_with_threads():
    threads = []

    # Launch threads to generate data and write to temp files
    for chunk_num in range(num_chunks):
        thread = threading.Thread(target=generate_and_write_chunk, args=(chunk_num,))
        threads.append(thread)
        thread.start()

        # Limit the number of concurrent threads to 50% of available cores
        if len(threads) >= num_threads:  # Use 50% of available cores
            for t in threads:
                t.join()  # Wait for all threads to finish before launching more
            threads = []

    # Wait for any remaining threads to complete
    for t in threads:
        t.join()

    # Combine temp files into the final CSV
    print(f"Combining {num_chunks} temporary files into '{file_name}'...")
    with open(file_name, 'w') as final_file:
        for chunk_num in range(num_chunks):
            temp_file = os.path.join(temp_dir, f'batch_{chunk_num}.csv')
            with open(temp_file, 'r') as batch_file:
                # Skip the header for all files except the first one
                if chunk_num != 0:
                    next(batch_file)
                # Write the content to the final file
                final_file.write(batch_file.read())

    # Cleanup temporary files
    print(f"Cleaning up temporary files...")
    for chunk_num in range(num_chunks):
        temp_file = os.path.join(temp_dir, f'batch_{chunk_num}.csv')
        os.remove(temp_file)

    print(f"Final CSV file '{file_name}' created successfully.")

if __name__ == '__main__':
    print(f"Using {num_threads} out of {num_cores} available cores (50% of cores)...")
    generate_large_csv_with_threads()
