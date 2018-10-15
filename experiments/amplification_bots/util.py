import math


# https://medium.com/@ssbothwell/counting-inversions-with-merge-sort-4d9910dc95f0
def merge_sort_inversions(arr):
    if len(arr) == 1:
        return arr, 0
    else:
        a = arr[:math.floor(len(arr)/2)]
        b = arr[math.floor(len(arr)/2):]
        a, ai = merge_sort_inversions(a)
        b, bi = merge_sort_inversions(b)
        c = []
        i = 0
        j = 0
        inversions = 0 + ai + bi
    while i < len(a) and j < len(b):
        if a[i] <= b[j]:
            c.append(a[i])
            i += 1
        else:
            c.append(b[j])
            j += 1
            inversions += (len(a)-i)
    c += a[i:]
    c += b[j:]

    return c, inversions
