def transposeMatrix(matrix):
    NROW = len(matrix[0])
    NCOL = len(matrix)
    
    new_matrix = [] * NROW
    for i in range(NROW):
        each_row_list = [] * NCOL
        for j in range(NCOL):
            each_row_list.append(matrix[j][i])
        new_matrix.append(each_row_list)
        
    return new_matrix



matrix = [
  [1, 2],
  [3, 4],
  [5, 6]
]

print(transposeMatrix(matrix))