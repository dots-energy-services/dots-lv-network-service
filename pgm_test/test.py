
import re

def extract_numbers(text):
    # This regex finds integers and decimal numbers
    numbers = re.findall(r'\d+\.?\d*', text)
    return [float(num) if '.' in num else int(num) for num in numbers]

# Example usage
sample_text = "The price is 45.99 dollars, and the discount is 10% on 3 items."
numbers = extract_numbers(sample_text)
print(numbers)
