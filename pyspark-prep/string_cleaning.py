# Given a string i.e '0906455467_venkat',
# remove all numbers along with '_' and consider only character words using a data frame.

import re

a_string = "0906455467_venkat"
pattern_to_replace = "[0-9_]"
print(re.sub(pattern_to_replace,"", a_string))

