import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt

# plot latency results as violing plot
class LatencyPlot(object):

    def __init__(self, data):
        # data is a dictionary mapping threads to lists of latencies

        # create pandas dataframe from data
        self.df = pd.DataFrame(data={'Thread' : data.keys(), 'Latency' : data.values()}).explode('Latency', ignore_index=True)
        self.df = self.df.astype({'Latency' : 'int64'})

    def show(self):
        sns.violinplot(data=self.df, x='Thread', y='Latency', inner='point')
        plt.show()


class FairnessPlot(object):

    def __init__(self, data):
        # data is a list of tuples (thread, time, weight)
        # (odd entries are start times, even entries are end times)

        thread, time, weight = zip(*data)

        # create pandas dataframe from data
        self.df = pd.DataFrame(data={'Thread'    : thread,
                                     'Real time' : time,
                                     'weight'    : weight}).sort_values(['Thread', 'Real time'])

        diff = self.df['Real time'].diff()
        diff[::2] = 0
        self.df['Virtual time'] = diff / self.df['weight']
        self.df['Virtual time'] = self.df.groupby('Thread')['Virtual time'].cumsum()

        # calculate gradient and sliding window
        gradient = self.df['Virtual time'].diff()
        gradient[::2] = 0
        self.df['Service rate'] = gradient.rolling(2).sum() / 2

    def show(self):
        fig, axs = plt.subplots(nrows=2)
        sns.lineplot(data=self.df, x='Real time', y='Virtual time', hue='Thread', ax=axs[0])
        sns.lineplot(data=self.df, x='Real time', y='Service rate', hue='Thread', ax=axs[1])
        plt.show()
