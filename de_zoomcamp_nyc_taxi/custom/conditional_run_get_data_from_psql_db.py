if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    get_data_from = kwargs['get_data_from']
    
    cab_type_mapping_list = {
        "yellow": False,
        "green": False,
        "fhv": False
    }
    
    # Update to True if the key exists in get_data_from
    for key in cab_type_mapping_list.keys():
        if key in get_data_from:
            cab_type_mapping_list[key] = True
    print(cab_type_mapping_list)
    return cab_type_mapping_list
