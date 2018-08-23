"""
    This is the class to handle the ticker dictionary information. Used to prevent duplicates
"""

def merge_dicts(*dict_list):
    """Extract all of the dictionaries from this list, then merge them together """
    # if not isinstance(dict_list, list):
    #     raise TypeError("dict_list is not a list. Please try again")
    # print(dict_list)
    all_dicts = []
    for ag in dict_list:
        if isinstance(ag, dict):
            all_dicts.append(ag)
    
    # Get all dictionaries
    # Merge them
    # Check if type is there
    try:
        qitem = { k: v for d in all_dicts for k, v in d.items() }
        return qitem
    except Exception:
        return {}
    


def filter_query(filter_dict, required_keys):
    """Ensure that the dict has all of the information available. If not, return what does"""
    if not isinstance(filter_dict, dict):
        raise TypeError("dict_list is not a list. Please try again")

    if not isinstance(required_keys, list):
        raise TypeError("dict_list is not a list. Please try again")

    available = []
    for k,v in filter_dict.items():
        print(k, v)
        if k in required_keys:
            available.append(k)
    return available


def price_insert_filter(price_dict):
    """
        Price has a set of requirements:
            - period -> [minute, hour, day]
            - exchange
            - open
            - close
            - high
            - low
            - volumefrom
            - volumeto
        Ensure that we have all of them for inserts. Will
    """
    if not isinstance(price_dict, dict):
        raise TypeError("dict_list is not a list. Please try again")
    required = ["period", "exchange", "open", "close", "high", "low", "volumefrom", "volumeto"]

    pkeys = price_dict.keys()
    for r in required:
        if r not in pkeys:
            raise KeyError("An important key is not available")
    return True

def price_query_filter(price_query_dict):
    """
        Ensure that certain keys are available.
            - exchange
            - period
    """
    if not isinstance(price_query_dict, dict):
        raise TypeError("dict_list is not a list. Please try again")
    required = ["period", "exchange"]
    pkeys = price_query_dict.keys()
    for r in required:
        if r not in pkeys:
            raise KeyError("An important key is not available {}".format(r))
    return True


# if __name__ == "__main__":
#     # print(filter_query({"type": "price"}, ["type"]))