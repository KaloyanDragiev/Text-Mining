# Import csv file and clean up the data

import csv
import random

data = []
# Open the file
with open('Data component 2.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)

    for line in csv_reader:
        text = ""
        for piece in line:
            text += piece + ","
        # Remove the last comma
        text = text[:-1]
        data.append(text)

# Remove '<br />' from the data
for i in range(len(data)):
    data[i] = data[i].replace('<br />', '')
    # Remove double spaces
    data[i] = data[i].replace('  ', ' ')

# Generate 200 random numbers between 0 and 3790
random.seed(1)
validation_numbers = random.sample(range(0, 3790), 200)

# Create validation set and training set
validation_set = []
training_set = []
for i in range(len(data)):
    if i in validation_numbers:
        validation_set.append(data[i])
    else:
        training_set.append(data[i])


# Create pandas dataframe
import pandas as pd
train_df = pd.DataFrame(training_set)
train_df.to_csv('Data component 2 - training.txt', index=False, header=False, quoting=csv.QUOTE_NONE, sep='|', escapechar='\\')
validation_df = pd.DataFrame(validation_set)
validation_df.to_csv('Data component 2 - validation.txt', index=False, header=False, quoting=csv.QUOTE_NONE, sep='|', escapechar='\\')
print(validation_numbers)

