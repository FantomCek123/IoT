import pandas as pd

originalne_kolone = ['date', 'time', 'epoch', 'deviceId', 'temperature', 'humidity', 'lightIntensity', 'voltage']

print("Čitam data.txt.gz i pravim JSON za k6...")

df = pd.read_csv('data.txt.gz', sep=' ', names=originalne_kolone, index_col=False)
df = df.dropna()
df['timestamp'] = df['date'] + ' ' + df['time']

konacne_kolone = ['deviceId', 'temperature', 'humidity', 'lightIntensity', 'voltage', 'timestamp']

df_za_k6 = df[konacne_kolone]
df_za_k6['deviceId'] = df_za_k6['deviceId'].apply(lambda x: f"Intel_Mote_{int(x)}")
df_za_k6.head(50000).to_json('intel_data.json', orient='records', indent=2)

print("Gotovo! Kreiran je fajl 'intel_data.json' spreman za k6.")