# Load necessary libraries
library(jsonlite)
library(dplyr)

# Step 1: Read the CSV file line by line
file_name = "txn_20221001_20240930.csv"
file_path <- paste("Data/", file_name, sep = '')
raw_lines <- readLines(file_path)

# Limit to the first 100 lines (including the header)
header <- raw_lines[1]  # Store the header separately
limited_lines <- c(header, raw_lines[2:min(101, length(raw_lines))])

# Step 2: Define a function to parse each line, separating first 9, middle, and last 9 columns
process_line <- function(line) {
  # Split the line by commas
  fields <- strsplit(line, ",")[[1]]
  
  # If the line has fewer than 18 columns, add NA values to reach 18
  if (length(fields) < 18) {
    fields <- c(fields, rep(NA, 18 - length(fields)))
  }
  
  # Extract the first 9 columns
  first_9 <- fields[1:9]
  
  # Extract the last 9 columns
  last_9 <- tail(fields, 9)
  
  # Combine the middle columns into a single string
  middle_columns <- fields[10:(length(fields) - 9)]
  middle_combined <- paste(middle_columns, collapse = " ")
  
  # Return the result as a single vector for this row
  return(c(first_9, middle_combined, last_9))
}

# Step 3: Apply the function to each line
processed_data <- lapply(limited_lines, process_line) # change raw_lines to limited_lines if only need top 100 samples

# Step 4: Convert the list of rows to a data frame
# We assume each processed row has exactly 19 elements (9 + 1 combined middle + 9)
df <- as.data.frame(do.call(rbind, processed_data), stringsAsFactors = FALSE)

# Step 6: Save the resulting data frame to a new CSV file
output_file_path <- paste("Result/processed_", file_name, sep = '')
write.csv(df, output_file_path, row.names = FALSE)

# Message to indicate completion
cat("Processed CSV file has been saved to:", output_file_path)