
# combine lists
def combine_lists(list1, list2):
    return list1 + list2
# Example usage:
list_a = [1, 2, 3]
list_b = [4, 5, 6]
print(combine_lists(list_a, list_b))  # Output: [1, 2, 3, 4, 5, 6]
list_a = ['a', 'b', 'c']
list_b = ['d', 'e', 'f']
print(combine_lists(list_a, list_b))  # Output: ['a', 'b', 'c', 'd', 'e', 'f']
list_a = []
list_b = [1, 2, 3]
print(combine_lists(list_a, list_b))  # Output: [1, 2, 3]
list_a = [1, 2, 3] 
list_b = []
print(combine_lists(list_a, list_b))  # Output: [1, 2, 3]
list_a = [1, 2, 2]
list_b = [2, 3, 4]
print(combine_lists(list_a, list_b))  # Output: [1, 2, 2, 2, 3, 4]
list_a = [None, 1, 2]
list_b = [2, None, 3]
print(combine_lists(list_a, list_b))  # Output: [None, 1, 2, 2, None, 3]
list_a = [1, 'a', None]
list_b = [None, 'a', 3]
print(combine_lists(list_a, list_b))  # Output: [1, 'a', None, None, 'a', 3]
list_a = [i for i in range(100)]
list_b = [i for i in range(100, 200)]
print(combine_lists(list_a, list_b))  # Output: [0, 1, 2, ..., 199]