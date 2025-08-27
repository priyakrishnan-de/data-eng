
# Count how many times each word is repeated in a sentence. Ignore special characters.
import re
from collections import Counter

def count_repeated_words(sentence): 
    # Remove special characters and convert to lowercase
    cleaned_sentence = re.sub(r'[^\w\s]', '', sentence).lower()
    
    # Split the sentence into words
    words = cleaned_sentence.split()
    
    # Count the occurrences of each word
    word_counts = Counter(words)
    
    # Filter and return only the words that are repeated
    repeated_words = {word: count for word, count in word_counts.items() if count > 1}
    
    return repeated_words

# Example usage
sentence = "This is a test. This test is only a test!"
result = count_repeated_words(sentence)
print(result)  # Output: {'this': 2, 'is': 2, 'a': 2, 'test': 3}

sentence = "The food was good. I would like to order some desert. What are some good desert options available. "
result = count_repeated_words(sentence)
print(result)  # Output: {'good': 2, 'desert': 2, 'some': 2}
sentence = "Python is great. Python is dynamic. Python is easy to learn."
result = count_repeated_words(sentence)
print(result)  # Output: {'python': 3, 'is': 3}