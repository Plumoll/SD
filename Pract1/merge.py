from collections import Counter

# d1 = {"a":1, "b":5, "c":3}
# d2 = {"b":2, "h":1}
# d3 = "I'm not a dictionary"

def dictionary_merge(*args):
    daux = {}
    for arg in args:
        if(isinstance(arg,dict)):
            df = Counter(daux) + Counter(arg)
            daux = df
        else:
            continue
        
    df = dict(daux)
    print(df)
        
# dictionary_merge(d1,d2,d3)
