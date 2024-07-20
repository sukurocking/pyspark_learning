def is_right_rotation(word1: str, word2: str) -> int:
    word1_list = list(word1)
    word1_length = len(word1)
    right_rotation_words = []

    # Defaulting return value to -1
    result = -1

    for i in range(word1_length):
        popped_element = word1_list[-1]
        word1_list.pop(-1)
        word1_list.insert(0,popped_element)
        right_rotation_word = "".join(word1_list)
        # print(right_rotation_word)
        right_rotation_words.append(right_rotation_word)

    if word2 in right_rotation_words:
        result = 1
        return result

    return result


if __name__ == "__main__":
    print(is_right_rotation("sample", "plesam"))
