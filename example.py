from connect import SEMRush
import pandas as pd

sem = SEMRush()
df = pd.read_csv("keywords.csv")
df = df[df["Keyword"].str.isdigit() == False].copy()
kws = list(df["Keyword"].unique())
data = sem.phrase_these(kws)
