import itertools

def cartesian_product(lists):
    result = []
    for items in itertools.product(*lists):
        flattened = [item for sublist in items for item in sublist]
        result.append(flattened)
    return result

print(cartesian_product([[['a','b'],[1,2]]]))