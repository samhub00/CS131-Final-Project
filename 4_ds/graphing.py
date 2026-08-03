import pandas as pd
import matplotlib.pyplot as plt

csdf = pd.read_csv("4_ds/graphing_data730.csv")
celdf = pd.read_csv("4_ds/graphing_data504230.csv")
drgdf = pd.read_csv("4_ds/graphing_data548430.csv")
terrdf = pd.read_csv("4_ds/graphing_data105600.csv")

dataframes = [csdf, terrdf, celdf, drgdf]
titles = ["Counter-Strike 2", "Terraria", "Celeste", "Deep Rock Galactic"]

fig, axes = plt.subplots(2, 2, figsize=(12, 10))
axes_flat = axes.flatten()
fig.suptitle("Review Length vs Sentiment for Selected Games", fontsize=16)

for i, df in enumerate(dataframes):
    df.columns = df.columns.str.strip()
    ax = axes_flat[i]
    df.plot(x='review_length', y=['0', '1'], ax=ax)
    ax.set_title(titles[i])
    ax.set_xlabel("Review Length")
    ax.set_ylabel("Count of Reviews")
    ax.legend(["Negative Reviews", "Positive Reviews"])
    ax.set_yscale('log')  # Set y-axis to logarithmic scale
    ax.set_xscale('log')  # Set x-axis to logarithmic scale

plt.tight_layout()

plt.savefig("4_ds/review_length_vs_sentiment.png", dpi=300, bbox_inches='tight')

plt.show()
#print(csdf.head(5))