import pandas as pd
test_data = [[
        'design_id', 'design_name', 'file_location', 'file_name'
    ],
    [{
        'design_id':'8',
        'design_name': 'Wooden',
        'file_location': '/usr',
        'file_name': 'wooden-20220717-npgz.json'
    },
    {
        'design_id':'7',
        'design_name': 'Woden',
        'file_location': '/us',
        'file_name': 'wooden.json'
    }]]
df_data = pd.DataFrame(data = test_data[1], columns = test_data[0]) 
print(df_data.values.tolist())