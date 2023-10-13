# Open the file for reading
import re

file_path = r"C:\Users\shivanandk\Desktop\except.txt"

pattern = r"(\d+)\+select count\(1\) (?:as )?Counts from \[([^\]]+)\]"
import pandas as pd

# Initialize an empty DataFrame with two columns
df = pd.DataFrame(columns=['Column1', 'Column2'])

# Number of rows you want to insert
num_rows = 200
# Open the file for reading
# file_path = "input.txt"  # Replace with the path to your text file

try:
    with open(file_path, 'r') as file:
        for line in file:
            match = re.search(pattern, line)
            if match:
                first_num = match.group(1)
                words_in_brackets = match.group(2)
                print(f"{first_num}>>>{words_in_brackets}")
                df = df.append({'Column1': first_num, 'Column2': words_in_brackets}, ignore_index=True)

                # print(f"First Number: {first_num}, Words in Brackets: {words_in_brackets}")
            else:
                print("No match found for line:", line)
except FileNotFoundError:
    print(f"File not found: {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")

file_path=r"""C:\Users\shivanandk\Desktop\tryoi.xlsx"""
df.to_excel(file_path)
print("completed..")



# # Use a loop to insert values into the DataFrame
# for i in range(1, num_rows + 1):
#     value1 = f"Value-{i}"  # Example value for Column1
#     value2 = i * 10  # Example value for Column2
#     df = df.append({'Column1': value1, 'Column2': value2}, ignore_index=True)

# # Print the DataFrame
# print(df)

