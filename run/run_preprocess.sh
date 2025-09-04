# Define variables
source .env
db_root_directory=$DB_ROOT_DIRECTORY # UPDATE THIS WITH THE PATH TO THE PARENT DIRECTORY OF THE DATABASES
db_id="chinook" # Options: all or a specific db_id
verbose=true
signature_size=100
n_gram=3
threshold=0.01


# --- ADD THIS SECTION ---
# Step 1: Generate database descriptions
if [ "$db_id" == "all" ]; then
    echo "Generating descriptions for all databases..."
    # You would need a loop here to iterate through all databases
    # This example assumes a single db_id for simplicity
    echo "Please generate descriptions for each database individually or adjust the script."
else
    echo "Generating description for $db_id"
    python3 -u ./src/create_description.py --db_id "${db_id}" # If you modify create_description.py to accept arguments
fi

# OR, if create_description.py is self-contained:
echo "Running description generator script..."
python3 -u ./create_description.py
# --- END OF ADDED SECTION ---



# Run the Python script with the defined variables
python3 -u ./src/preprocess.py --db_root_directory "${db_root_directory}" \
                              --signature_size "${signature_size}" \
                              --n_gram "${n_gram}" \
                              --threshold "${threshold}" \
                              --db_id "${db_id}" \
                              --verbose "${verbose}"
