import pandas as pd

# File paths
csv_file_path = "../datasource/Landslides.csv"
excel_file_path = "../datasource/Global_Landslide_Data.xlsx"

# Read the entire CSV and Excel files
csv_data = pd.read_csv(csv_file_path)
excel_data = pd.read_excel(excel_file_path)

# Define the proportions for each part
dev_proportion = 0.1  # 10% for development
test_proportion = 0.2  # 20% for testing
presentation_proportion = 0.1  # 10% for presentation

# Calculate the number of rows for each part
csv_dev_rows = int(len(csv_data) * dev_proportion)
csv_test_rows = int(len(csv_data) * test_proportion)
csv_presentation_rows = int(len(csv_data) * presentation_proportion)

excel_dev_rows = int(len(excel_data) * dev_proportion)
excel_test_rows = int(len(excel_data) * test_proportion)
excel_presentation_rows = int(len(excel_data) * presentation_proportion)

# Split the data into parts
csv_dev = csv_data[:csv_dev_rows]
csv_test = csv_data[csv_dev_rows:csv_dev_rows + csv_test_rows]
csv_running = csv_data[csv_dev_rows + csv_test_rows:]
csv_presentation = csv_data[:csv_presentation_rows]

excel_dev = excel_data[:excel_dev_rows]
excel_test = excel_data[excel_dev_rows:excel_dev_rows + excel_test_rows]
excel_running = excel_data[excel_dev_rows + excel_test_rows:]
excel_presentation = excel_data[:excel_presentation_rows]

# Write the parts to new files
csv_dev.to_csv("dev_sample.csv", index=False)
csv_test.to_csv("test_sample.csv", index=False)
csv_running.to_csv("production_data.csv", index=False)
csv_presentation.to_csv("presentation_sample.csv", index=False)

excel_dev.to_excel("dev_sample.xlsx", index=False)
excel_test.to_excel("test_sample.xlsx", index=False)
excel_running.to_excel("production_data.xlsx", index=False)
excel_presentation.to_excel("presentation_sample.xlsx", index=False)

print("Data division completed successfully!")
