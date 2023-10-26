import re

# Sample dataset with phone numbers
dataset = [
    "123-456-7890",
    "(987) 654-3210",
    "555.123.4567",
    "1234567890",
    "555-555-5555",
]


# Function to transform phone numbers
def transform_phone_number(phone_number):
    # Define a regular expression pattern to match digits in the phone number
    digit_pattern = r'\d'

    # Find all the digits in the phone number
    digits = re.findall(digit_pattern, phone_number)

    # Check if there are at least 8 digits in the phone number
    if len(digits) >= 8:
        # Replace 8 of the digits with '*'
        transformed_number = re.sub(digit_pattern, '*', phone_number, count=8)
        return transformed_number
    else:
        # If there are fewer than 8 digits, return the original number
        return phone_number


# Transform phone numbers in the dataset
transformed_dataset = [transform_phone_number(phone) for phone in dataset]

# Print the transformed dataset
for i, phone in enumerate(transformed_dataset):
    print(f"Original: {dataset[i]}, Transformed: {phone}")
