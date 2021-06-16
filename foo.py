import re
import pandas as pd

df = pd.DataFrame(['sp 1','cardinals', 'sp. foo'], columns=['P'])
print(df)

df['P'] = df['P'].str.replace(r'sp\..*', 'sp.')
df['P'] = df['P'].str.replace(r'sp .*', 'sp.')
print(df)
#p = "sp. Translate"
#p = re.sub(r'sp.*','sp.',p)
#print(p)

       #         thisDF['genus'] = re.sub(r'\[property\]','sp.',thisDF['genus'])
