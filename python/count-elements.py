
#count the number of elements in a list
# Input: a list of elements
# Output: the count of elements in the list as an integer
def count_elements(input_list):
    return len(input_list)  
# Example usage:
my_list = [1, 2, 3, 4, 5]
print(count_elements(my_list))  # Output: 5
my_list = []
print(count_elements(my_list))  # Output: 0
my_list = ['a', 'b', 'c']
print(count_elements(my_list))  # Output: 3
my_list = [1]
print(count_elements(my_list))  # Output: 1
my_list = [None, None, None]
print(count_elements(my_list))  # Output: 3
my_list = [1, 'a', None, 3.14]
print(count_elements(my_list))  # Output: 4
my_list = list(range(100))
print(count_elements(my_list))  # Output: 100
my_list = [1, 2, 3, 4, 5] * 20
print(count_elements(my_list))  # Output: 100
my_list = [i for i in range(1000)]
print(count_elements(my_list))  # Output: 1000
my_list = [i for i in range(10000)]
print(count_elements(my_list))  # Output: 10000
my_list = [i for i in range(100000)]
print(count_elements(my_list))  # Output: 100000
my_list = [i for i in range(1000000)]
print(count_elements(my_list))  # Output: 1000000
my_list = [i for i in range(10000000)] 