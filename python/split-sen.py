
# Split a string of a sentence into lists of 2 words

def split_sentence(sentence):

    words = sentence.split()
    return [words[i:i + 2] for i in range(0, len(words), 2)]
# Example usage
sentence = "This is an example sentence for splitting"
result = split_sentence(sentence)

print(result)  # Output: [['This', 'is'], ['an', 'example'], ['sentence', 'for'], ['splitting']]

sentence = "Today is a good day to learn Python"
result = split_sentence(sentence)

print(result)  # Output: [['Today', 'is'], ['a', 'good'], ['day', 'to'], ['learn', 'Python']]

